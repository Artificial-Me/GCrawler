"""
Persistent Queue Manager for GhostCrawler

Provides a robust task queue system with SQLite persistence, task prioritization,
and automatic retry capabilities with exponential backoff.
"""

import sqlite3
import asyncio
import logging
import json
import time
import random
import math
from enum import Enum, auto
from typing import List, Dict, Any, Optional, Tuple, Union
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

class TaskStatus(Enum):
    """Task status enum"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY = "retry"

class TaskPriority(Enum):
    """Task priority enum"""
    HIGH = 1
    MEDIUM = 2
    LOW = 3

class QueueManagerError(Exception):
    """Base exception for queue manager errors"""
    pass

class TaskQueue:
    """
    Persistent task queue manager using SQLite
    
    Provides methods to:
    - Add tasks to queue
    - Get next task
    - Mark task as completed or failed
    - Resume from crash
    - Get queue statistics
    """
    
    def __init__(self, db_path: str = "queue.db", max_retries: int = 3, 
                 base_retry_delay: float = 5.0):
        """
        Initialize the task queue
        
        Args:
            db_path: Path to SQLite database file
            max_retries: Maximum number of retries for failed tasks
            base_retry_delay: Base delay for retry backoff in seconds
        """
        self.db_path = db_path
        self.max_retries = max_retries
        self.base_retry_delay = base_retry_delay
        self._lock = asyncio.Lock()
        self._setup_database()
        logger.info(f"TaskQueue initialized with database at {db_path}")
        
        # Recover any tasks that were left in processing state
        self._recover_processing_tasks()
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection with row factory"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _setup_database(self) -> None:
        """Set up the database schema if it doesn't exist"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Create tasks table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                priority INTEGER NOT NULL,
                status TEXT NOT NULL,
                retry_count INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                last_attempted_at TEXT,
                next_retry_at TEXT,
                completed_at TEXT,
                error_message TEXT,
                metadata TEXT
            )
            ''')
            
            # Create indexes for faster queries
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_priority ON tasks(priority)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_url ON tasks(url)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_next_retry ON tasks(next_retry_at)')
            
            # Create statistics table for tracking overall queue metrics
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS queue_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                pending_count INTEGER NOT NULL,
                processing_count INTEGER NOT NULL,
                completed_count INTEGER NOT NULL,
                failed_count INTEGER NOT NULL,
                retry_count INTEGER NOT NULL,
                total_count INTEGER NOT NULL
            )
            ''')
            
            conn.commit()
            logger.debug("Database schema setup complete")
        except sqlite3.Error as e:
            logger.error(f"Database setup error: {e}")
            raise QueueManagerError(f"Failed to set up database: {e}")
        finally:
            if conn:
                conn.close()
    
    def _recover_processing_tasks(self) -> None:
        """Recover tasks that were left in processing state due to crash"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Get count of tasks in processing state
            cursor.execute('SELECT COUNT(*) as count FROM tasks WHERE status = ?', 
                          (TaskStatus.PROCESSING.value,))
            processing_count = cursor.fetchone()['count']
            
            if processing_count > 0:
                logger.warning(f"Found {processing_count} tasks in processing state. Recovering...")
                
                # Reset processing tasks to pending or retry based on retry count
                cursor.execute('''
                UPDATE tasks 
                SET status = CASE
                    WHEN retry_count >= ? THEN ?
                    ELSE ?
                END,
                error_message = CASE
                    WHEN retry_count >= ? THEN 'Failed due to exceeding retry limit after crash recovery'
                    ELSE 'Task recovered from processing state after crash'
                END,
                retry_count = CASE
                    WHEN retry_count < ? THEN retry_count + 1
                    ELSE retry_count
                END
                WHERE status = ?
                ''', (
                    self.max_retries, TaskStatus.FAILED.value, TaskStatus.RETRY.value,
                    self.max_retries, self.max_retries, TaskStatus.PROCESSING.value
                ))
                
                recovered = cursor.rowcount
                conn.commit()
                logger.info(f"Recovered {recovered} tasks from processing state")
        except sqlite3.Error as e:
            logger.error(f"Error recovering processing tasks: {e}")
        finally:
            if conn:
                conn.close()
    
    async def add_task(self, url: str, priority: Union[TaskPriority, int] = TaskPriority.MEDIUM, 
                      metadata: Optional[Dict[str, Any]] = None) -> int:
        """
        Add a task to the queue
        
        Args:
            url: URL to crawl
            priority: Task priority (TaskPriority enum or int 1-3)
            metadata: Additional task metadata as dictionary
            
        Returns:
            Task ID
        """
        async with self._lock:
            try:
                # Convert priority enum to int if needed
                if isinstance(priority, TaskPriority):
                    priority_value = priority.value
                else:
                    priority_value = int(priority)
                    if priority_value < 1 or priority_value > 3:
                        priority_value = TaskPriority.MEDIUM.value
                
                conn = self._get_connection()
                cursor = conn.cursor()
                
                # Check if URL already exists in pending or retry state
                cursor.execute('''
                SELECT id, status FROM tasks 
                WHERE url = ? AND status IN (?, ?)
                ''', (url, TaskStatus.PENDING.value, TaskStatus.RETRY.value))
                
                existing = cursor.fetchone()
                if existing:
                    logger.debug(f"Task for URL {url} already exists with ID {existing['id']}")
                    return existing['id']
                
                # Serialize metadata to JSON if provided
                metadata_json = json.dumps(metadata) if metadata else None
                
                # Insert new task
                now = datetime.now().isoformat()
                cursor.execute('''
                INSERT INTO tasks (url, priority, status, created_at, metadata)
                VALUES (?, ?, ?, ?, ?)
                ''', (url, priority_value, TaskStatus.PENDING.value, now, metadata_json))
                
                task_id = cursor.lastrowid
                conn.commit()
                logger.debug(f"Added task {task_id} for URL {url} with priority {priority_value}")
                return task_id
            except sqlite3.Error as e:
                logger.error(f"Error adding task for URL {url}: {e}")
                raise QueueManagerError(f"Failed to add task: {e}")
            finally:
                if conn:
                    conn.close()
    
    async def add_tasks_bulk(self, tasks: List[Dict[str, Any]]) -> List[int]:
        """
        Add multiple tasks to the queue in bulk
        
        Args:
            tasks: List of task dictionaries with keys:
                  - url (required)
                  - priority (optional)
                  - metadata (optional)
                  
        Returns:
            List of task IDs
        """
        async with self._lock:
            task_ids = []
            conn = None
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                # Begin transaction
                conn.execute('BEGIN TRANSACTION')
                
                now = datetime.now().isoformat()
                for task in tasks:
                    url = task['url']
                    
                    # Convert priority enum to int if needed
                    priority = task.get('priority', TaskPriority.MEDIUM)
                    if isinstance(priority, TaskPriority):
                        priority_value = priority.value
                    else:
                        priority_value = int(priority) if isinstance(priority, int) else TaskPriority.MEDIUM.value
                        if priority_value < 1 or priority_value > 3:
                            priority_value = TaskPriority.MEDIUM.value
                    
                    # Serialize metadata to JSON if provided
                    metadata = task.get('metadata')
                    metadata_json = json.dumps(metadata) if metadata else None
                    
                    # Check if URL already exists in pending or retry state
                    cursor.execute('''
                    SELECT id FROM tasks 
                    WHERE url = ? AND status IN (?, ?)
                    ''', (url, TaskStatus.PENDING.value, TaskStatus.RETRY.value))
                    
                    existing = cursor.fetchone()
                    if existing:
                        task_ids.append(existing['id'])
                        continue
                    
                    # Insert new task
                    cursor.execute('''
                    INSERT INTO tasks (url, priority, status, created_at, metadata)
                    VALUES (?, ?, ?, ?, ?)
                    ''', (url, priority_value, TaskStatus.PENDING.value, now, metadata_json))
                    
                    task_ids.append(cursor.lastrowid)
                
                conn.commit()
                logger.info(f"Added {len(task_ids)} tasks in bulk")
                return task_ids
            except sqlite3.Error as e:
                if conn:
                    conn.rollback()
                logger.error(f"Error adding tasks in bulk: {e}")
                raise QueueManagerError(f"Failed to add tasks in bulk: {e}")
            finally:
                if conn:
                    conn.close()
    
    async def get_next_task(self) -> Optional[Dict[str, Any]]:
        """
        Get the next task from the queue based on priority and retry time
        
        Returns:
            Task dictionary or None if no tasks available
        """
        async with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                now = datetime.now().isoformat()
                
                # Get next task with priority ordering
                # First check pending tasks, then retry tasks that are ready
                cursor.execute('''
                SELECT id, url, priority, retry_count, created_at, metadata
                FROM tasks
                WHERE (status = ? OR (status = ? AND next_retry_at <= ?))
                ORDER BY 
                    CASE status
                        WHEN ? THEN 0  -- Pending tasks first
                        WHEN ? THEN 1  -- Retry tasks second
                    END,
                    priority ASC,      -- Higher priority (lower number) first
                    created_at ASC     -- Older tasks first
                LIMIT 1
                ''', (
                    TaskStatus.PENDING.value, 
                    TaskStatus.RETRY.value,
                    now,
                    TaskStatus.PENDING.value,
                    TaskStatus.RETRY.value
                ))
                
                row = cursor.fetchone()
                if not row:
                    return None
                
                task_id = row['id']
                
                # Mark task as processing
                cursor.execute('''
                UPDATE tasks
                SET status = ?, last_attempted_at = ?
                WHERE id = ?
                ''', (TaskStatus.PROCESSING.value, now, task_id))
                
                conn.commit()
                
                # Convert row to dictionary
                task = dict(row)
                
                # Parse metadata JSON
                if task.get('metadata'):
                    try:
                        task['metadata'] = json.loads(task['metadata'])
                    except json.JSONDecodeError:
                        task['metadata'] = {}
                
                logger.debug(f"Retrieved task {task_id} for processing")
                return task
            except sqlite3.Error as e:
                logger.error(f"Error getting next task: {e}")
                return None
            finally:
                if conn:
                    conn.close()
    
    async def get_tasks_batch(self, batch_size: int = 10) -> List[Dict[str, Any]]:
        """
        Get a batch of tasks from the queue based on priority and retry time
        
        Args:
            batch_size: Number of tasks to retrieve
            
        Returns:
            List of task dictionaries
        """
        async with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                now = datetime.now().isoformat()
                
                # Get batch of tasks with priority ordering
                cursor.execute('''
                SELECT id, url, priority, retry_count, created_at, metadata
                FROM tasks
                WHERE (status = ? OR (status = ? AND next_retry_at <= ?))
                ORDER BY 
                    CASE status
                        WHEN ? THEN 0  -- Pending tasks first
                        WHEN ? THEN 1  -- Retry tasks second
                    END,
                    priority ASC,      -- Higher priority (lower number) first
                    created_at ASC     -- Older tasks first
                LIMIT ?
                ''', (
                    TaskStatus.PENDING.value, 
                    TaskStatus.RETRY.value,
                    now,
                    TaskStatus.PENDING.value,
                    TaskStatus.RETRY.value,
                    batch_size
                ))
                
                rows = cursor.fetchall()
                if not rows:
                    return []
                
                task_ids = [row['id'] for row in rows]
                id_placeholders = ','.join(['?'] * len(task_ids))
                
                # Mark tasks as processing
                cursor.execute(f'''
                UPDATE tasks
                SET status = ?, last_attempted_at = ?
                WHERE id IN ({id_placeholders})
                ''', [TaskStatus.PROCESSING.value, now] + task_ids)
                
                conn.commit()
                
                # Convert rows to dictionaries
                tasks = []
                for row in rows:
                    task = dict(row)
                    
                    # Parse metadata JSON
                    if task.get('metadata'):
                        try:
                            task['metadata'] = json.loads(task['metadata'])
                        except json.JSONDecodeError:
                            task['metadata'] = {}
                    
                    tasks.append(task)
                
                logger.debug(f"Retrieved batch of {len(tasks)} tasks for processing")
                return tasks
            except sqlite3.Error as e:
                logger.error(f"Error getting tasks batch: {e}")
                return []
            finally:
                if conn:
                    conn.close()
    
    async def mark_task_completed(self, task_id: int, result: Optional[Dict[str, Any]] = None) -> bool:
        """
        Mark a task as completed
        
        Args:
            task_id: Task ID
            result: Optional result data
            
        Returns:
            Success status
        """
        async with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                now = datetime.now().isoformat()
                result_json = json.dumps(result) if result else None
                
                cursor.execute('''
                UPDATE tasks
                SET status = ?, completed_at = ?, metadata = ?
                WHERE id = ? AND status = ?
                ''', (
                    TaskStatus.COMPLETED.value, 
                    now, 
                    result_json,
                    task_id, 
                    TaskStatus.PROCESSING.value
                ))
                
                if cursor.rowcount == 0:
                    logger.warning(f"Task {task_id} not found or not in processing state")
                    return False
                
                conn.commit()
                logger.debug(f"Marked task {task_id} as completed")
                return True
            except sqlite3.Error as e:
                logger.error(f"Error marking task {task_id} as completed: {e}")
                return False
            finally:
                if conn:
                    conn.close()
    
    async def mark_task_failed(self, task_id: int, error_message: str) -> bool:
        """
        Mark a task as failed and handle retry logic
        
        Args:
            task_id: Task ID
            error_message: Error message
            
        Returns:
            Success status
        """
        async with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                # Get current task info
                cursor.execute('''
                SELECT retry_count, priority FROM tasks
                WHERE id = ? AND status = ?
                ''', (task_id, TaskStatus.PROCESSING.value))
                
                row = cursor.fetchone()
                if not row:
                    logger.warning(f"Task {task_id} not found or not in processing state")
                    return False
                
                retry_count = row['retry_count']
                priority = row['priority']
                
                now = datetime.now()
                now_iso = now.isoformat()
                
                # Check if we should retry
                if retry_count < self.max_retries:
                    # Calculate next retry time with exponential backoff and jitter
                    retry_count += 1
                    delay = self._calculate_retry_delay(retry_count, priority)
                    next_retry = (now + timedelta(seconds=delay)).isoformat()
                    
                    cursor.execute('''
                    UPDATE tasks
                    SET status = ?, retry_count = ?, error_message = ?, next_retry_at = ?
                    WHERE id = ?
                    ''', (
                        TaskStatus.RETRY.value,
                        retry_count,
                        error_message,
                        next_retry,
                        task_id
                    ))
                    
                    logger.debug(f"Task {task_id} failed, scheduled for retry #{retry_count} at {next_retry}")
                else:
                    # Mark as permanently failed
                    cursor.execute('''
                    UPDATE tasks
                    SET status = ?, error_message = ?, completed_at = ?
                    WHERE id = ?
                    ''', (
                        TaskStatus.FAILED.value,
                        f"{error_message} (Exceeded max retries: {self.max_retries})",
                        now_iso,
                        task_id
                    ))
                    
                    logger.warning(f"Task {task_id} permanently failed after {retry_count} retries: {error_message}")
                
                conn.commit()
                return True
            except sqlite3.Error as e:
                logger.error(f"Error marking task {task_id} as failed: {e}")
                return False
            finally:
                if conn:
                    conn.close()
    
    def _calculate_retry_delay(self, retry_count: int, priority: int) -> float:
        """
        Calculate retry delay with exponential backoff, priority adjustment, and jitter
        
        Args:
            retry_count: Current retry count
            priority: Task priority (1=high, 3=low)
            
        Returns:
            Delay in seconds
        """
        # Base exponential backoff
        delay = self.base_retry_delay * (2 ** (retry_count - 1))
        
        # Priority adjustment (higher priority = less delay)
        priority_factor = 1.0 + ((priority - 1) * 0.5)  # 1.0 for high, 1.5 for medium, 2.0 for low
        delay *= priority_factor
        
        # Add jitter (±15%)
        jitter = random.uniform(-0.15, 0.15)
        delay *= (1 + jitter)
        
        # Cap at reasonable maximum (30 minutes)
        return min(delay, 1800)
    
    async def get_queue_stats(self) -> Dict[str, int]:
        """
        Get current queue statistics
        
        Returns:
            Dictionary with queue statistics
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            stats = {}
            
            # Get counts by status
            for status in TaskStatus:
                cursor.execute('SELECT COUNT(*) as count FROM tasks WHERE status = ?', 
                              (status.value,))
                stats[status.name.lower()] = cursor.fetchone()['count']
            
            # Get counts by priority
            for priority in TaskPriority:
                cursor.execute('SELECT COUNT(*) as count FROM tasks WHERE priority = ?', 
                              (priority.value,))
                stats[f"priority_{priority.name.lower()}"] = cursor.fetchone()['count']
            
            # Get total count
            cursor.execute('SELECT COUNT(*) as count FROM tasks')
            stats['total'] = cursor.fetchone()['count']
            
            # Get retry stats
            cursor.execute('SELECT AVG(retry_count) as avg_retries FROM tasks WHERE retry_count > 0')
            stats['avg_retries'] = round(cursor.fetchone()['avg_retries'] or 0, 2)
            
            # Save stats to history table
            now = datetime.now().isoformat()
            cursor.execute('''
            INSERT INTO queue_stats 
            (timestamp, pending_count, processing_count, completed_count, 
             failed_count, retry_count, total_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                now, 
                stats['pending'],
                stats['processing'],
                stats['completed'],
                stats['failed'],
                stats['retry'],
                stats['total']
            ))
            
            conn.commit()
            return stats
        except sqlite3.Error as e:
            logger.error(f"Error getting queue stats: {e}")
            return {}
        finally:
            if conn:
                conn.close()
    
    async def get_stats_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get queue statistics history
        
        Args:
            limit: Maximum number of history entries to return
            
        Returns:
            List of statistics dictionaries
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT * FROM queue_stats
            ORDER BY timestamp DESC
            LIMIT ?
            ''', (limit,))
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except sqlite3.Error as e:
            logger.error(f"Error getting stats history: {e}")
            return []
        finally:
            if conn:
                conn.close()
    
    async def purge_completed_tasks(self, older_than_days: int = 7) -> int:
        """
        Purge completed tasks older than specified days
        
        Args:
            older_than_days: Remove completed tasks older than this many days
            
        Returns:
            Number of tasks purged
        """
        async with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                cutoff_date = (datetime.now() - timedelta(days=older_than_days)).isoformat()
                
                cursor.execute('''
                DELETE FROM tasks
                WHERE status = ? AND completed_at < ?
                ''', (TaskStatus.COMPLETED.value, cutoff_date))
                
                purged = cursor.rowcount
                conn.commit()
                
                logger.info(f"Purged {purged} completed tasks older than {older_than_days} days")
                return purged
            except sqlite3.Error as e:
                logger.error(f"Error purging completed tasks: {e}")
                return 0
            finally:
                if conn:
                    conn.close()
    
    async def reset_stuck_tasks(self, stuck_minutes: int = 60) -> int:
        """
        Reset tasks that have been stuck in processing state
        
        Args:
            stuck_minutes: Consider tasks stuck if processing for more than this many minutes
            
        Returns:
            Number of tasks reset
        """
        async with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                cutoff_time = (datetime.now() - timedelta(minutes=stuck_minutes)).isoformat()
                
                cursor.execute('''
                UPDATE tasks
                SET status = CASE
                        WHEN retry_count >= ? THEN ?
                        ELSE ?
                    END,
                    error_message = ?,
                    retry_count = CASE
                        WHEN retry_count < ? THEN retry_count + 1
                        ELSE retry_count
                    END
                WHERE status = ? AND last_attempted_at < ?
                ''', (
                    self.max_retries, 
                    TaskStatus.FAILED.value, 
                    TaskStatus.RETRY.value,
                    f"Task reset after being stuck in processing state for more than {stuck_minutes} minutes",
                    self.max_retries,
                    TaskStatus.PROCESSING.value,
                    cutoff_time
                ))
                
                reset = cursor.rowcount
                conn.commit()
                
                if reset > 0:
                    logger.warning(f"Reset {reset} tasks stuck in processing state for more than {stuck_minutes} minutes")
                
                return reset
            except sqlite3.Error as e:
                logger.error(f"Error resetting stuck tasks: {e}")
                return 0
            finally:
                if conn:
                    conn.close()
    
    async def clear_queue(self, status: Optional[TaskStatus] = None) -> int:
        """
        Clear the queue, optionally filtering by status
        
        Args:
            status: Only clear tasks with this status (or all if None)
            
        Returns:
            Number of tasks cleared
        """
        async with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                if status:
                    cursor.execute('DELETE FROM tasks WHERE status = ?', (status.value,))
                    cleared = cursor.rowcount
                    logger.warning(f"Cleared {cleared} tasks with status {status.name}")
                else:
                    cursor.execute('DELETE FROM tasks')
                    cleared = cursor.rowcount
                    logger.warning(f"Cleared entire queue ({cleared} tasks)")
                
                conn.commit()
                return cleared
            except sqlite3.Error as e:
                logger.error(f"Error clearing queue: {e}")
                return 0
            finally:
                if conn:
                    conn.close()
    
    async def get_task_by_id(self, task_id: int) -> Optional[Dict[str, Any]]:
        """
        Get task by ID
        
        Args:
            task_id: Task ID
            
        Returns:
            Task dictionary or None if not found
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM tasks WHERE id = ?', (task_id,))
            row = cursor.fetchone()
            
            if not row:
                return None
            
            task = dict(row)
            
            # Parse metadata JSON
            if task.get('metadata'):
                try:
                    task['metadata'] = json.loads(task['metadata'])
                except json.JSONDecodeError:
                    task['metadata'] = {}
            
            return task
        except sqlite3.Error as e:
            logger.error(f"Error getting task {task_id}: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    async def get_failed_tasks(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get list of failed tasks
        
        Args:
            limit: Maximum number of tasks to return
            
        Returns:
            List of failed task dictionaries
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT * FROM tasks 
            WHERE status = ?
            ORDER BY completed_at DESC
            LIMIT ?
            ''', (TaskStatus.FAILED.value, limit))
            
            rows = cursor.fetchall()
            
            tasks = []
            for row in rows:
                task = dict(row)
                
                # Parse metadata JSON
                if task.get('metadata'):
                    try:
                        task['metadata'] = json.loads(task['metadata'])
                    except json.JSONDecodeError:
                        task['metadata'] = {}
                
                tasks.append(task)
            
            return tasks
        except sqlite3.Error as e:
            logger.error(f"Error getting failed tasks: {e}")
            return []
        finally:
            if conn:
                conn.close()
    
    async def backup_database(self, backup_path: Optional[str] = None) -> bool:
        """
        Create a backup of the queue database
        
        Args:
            backup_path: Path for backup file (default: queue_backup_TIMESTAMP.db)
            
        Returns:
            Success status
        """
        if not backup_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"queue_backup_{timestamp}.db"
        
        try:
            # Ensure the database is not being written to during backup
            async with self._lock:
                # Use SQLite's backup API via Python's connection objects
                source_conn = sqlite3.connect(self.db_path)
                dest_conn = sqlite3.connect(backup_path)
                
                source_conn.backup(dest_conn)
                
                source_conn.close()
                dest_conn.close()
                
                logger.info(f"Database backed up to {backup_path}")
                return True
        except sqlite3.Error as e:
            logger.error(f"Error backing up database: {e}")
            return False
