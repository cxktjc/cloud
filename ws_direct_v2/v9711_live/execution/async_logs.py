"""Preserve every trade log while file/console I/O runs off sender threads."""
import logging
import queue
from logging.handlers import QueueHandler, QueueListener


class AsyncTradeLogs:
    def __init__(self, path, level, console_filter):
        self.root = logging.getLogger()
        self.previous_handlers, self.previous_level = self.root.handlers[:], self.root.level
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
        console = logging.StreamHandler()
        console.addFilter(console_filter)
        disk = logging.FileHandler(path, encoding='utf-8')
        self.outputs = (console, disk)
        for handler in self.outputs:
            handler.setFormatter(formatter)
        records = queue.SimpleQueue()
        self.listener = QueueListener(records, *self.outputs, respect_handler_level=True)
        self.root.handlers = [QueueHandler(records)]
        self.root.setLevel(level)
        self.listener.start()

    def close(self):
        self.listener.stop()  # preserve queued log records through normal shutdown
        self.root.handlers = self.previous_handlers
        self.root.setLevel(self.previous_level)
        for handler in self.outputs:
            handler.close()
