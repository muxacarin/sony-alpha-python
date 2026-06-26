import logging


class EventHandler:
    # Event System
    def emit(self, event: str, *args):
        """Emit event to handlers"""
        logging.debug(f"Emitting event: {event}")
        if event in self._event_handlers:
            for handler in self._event_handlers[event][:]:
                try:
                    handler(*args)
                except Exception as e:
                    logging.error(f"Error in event handler for {event}: {e}")

    def on(self, event: str, handler):
        """Add event handler"""
        logging.debug(f"Adding event handler {handler.__name__} for event: {event}")
        if event not in self._event_handlers:
            self._event_handlers[event] = []
        self._event_handlers[event].append(handler)

    def once(self, event: str, handler):
        """Add one-time event handler"""

        def wrapper(*args):
            handler(*args)
            if event in self._event_handlers and wrapper in self._event_handlers[event]:
                self._event_handlers[event].remove(wrapper)

        self.on(event, wrapper)
