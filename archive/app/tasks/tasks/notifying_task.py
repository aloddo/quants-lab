"""
Mixin that adds Telegram failure notifications to any BaseTask.
Use as: class MyTask(NotifyingTaskMixin, BaseTask)

Keeps core/ untouched per project rules.
"""
import logging

from core.tasks import BaseTask, TaskContext

logger = logging.getLogger(__name__)


class NotifyingTaskMixin:
    """Mixin that sends Telegram on task failure. Add as first parent class."""

    async def on_failure(self, context: TaskContext, result) -> None:
        """Send Telegram alert on failure, then call parent on_failure."""
        if hasattr(self, "notification_manager") and self.notification_manager:
            try:
                from core.notifiers.base import NotificationMessage
                task_name = self.config.name if hasattr(self, "config") else "unknown"
                error_msg = getattr(result, "error_message", str(result))
                exec_id = getattr(context, "execution_id", "?")
                await self.notification_manager.send_notification(NotificationMessage(
                    title=f"Task Failed: {task_name}",
                    message=(
                        f"<b>Task Failure</b>\n"
                        f"Task: {task_name}\n"
                        f"Error: {error_msg}\n"
                        f"Execution: {exec_id}"
                    ),
                    level="error",
                ))
            except Exception as e:
                logger.debug(f"Failed to send failure notification: {e}")

        # Call next in MRO (BaseTask.on_failure)
        await super().on_failure(context, result)
