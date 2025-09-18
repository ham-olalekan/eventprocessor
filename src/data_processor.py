"""Data processor for grouping events by clientId."""

import logging
from typing import List, Dict, Any
from collections import defaultdict

logger = logging.getLogger(__name__)


class DataProcessor:
    """Process and group events by clientId."""

    def __init__(self):
        self.stats = {"total_events": 0, "unique_clients": 0, "events_per_client": {}}

    def group_events_by_client(
        self, events: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        grouped_events = defaultdict(list)

        for event in events:
            client_id = event.get("clientId")
            if not client_id:
                logger.warning(
                    f"Event missing clientId: {event.get('eventId', 'unknown')}"
                )
                continue

            grouped_events[client_id].append(event)

        self.stats["total_events"] = len(events)
        self.stats["unique_clients"] = len(grouped_events)
        self.stats["events_per_client"] = {
            client_id: len(events_list)
            for client_id, events_list in grouped_events.items()
        }

        self._log_statistics()

        return dict(grouped_events)

    def validate_event(self, event: Dict[str, Any]) -> bool:
        required_fields = ["eventId", "clientId", "time"]

        for field in required_fields:
            if field not in event or event[field] is None:
                logger.warning(f"Event missing required field '{field}': {event}")
                return False

        return True

    def filter_valid_events(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        valid_events = []
        invalid_count = 0

        for event in events:
            if self.validate_event(event):
                valid_events.append(event)
            else:
                invalid_count += 1

        if invalid_count > 0:
            logger.warning(f"Filtered out {invalid_count} invalid events")

        return valid_events

    def sort_events_by_time(
        self, grouped_events: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        sorted_groups = {}

        for client_id, events in grouped_events.items():
            sorted_events = sorted(events, key=lambda x: x.get("time", ""))
            sorted_groups[client_id] = sorted_events

        return sorted_groups

    def _log_statistics(self):
        logger.info(f"Processing statistics:")
        logger.info(f"  Total events: {self.stats['total_events']}")
        logger.info(f"  Unique clients: {self.stats['unique_clients']}")

        if self.stats["events_per_client"]:
            top_clients = sorted(
                self.stats["events_per_client"].items(),
                key=lambda x: x[1],
                reverse=True,
            )[:5]

            logger.info("  Top clients by event count:")
            for client_id, count in top_clients:
                logger.info(f"    {client_id}: {count} events")

    def get_statistics(self) -> Dict[str, Any]:
        return self.stats.copy()

    def process_events(
        self, events: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        logger.info(f"Starting to process {len(events)} events")

        valid_events = self.filter_valid_events(events)

        grouped_events = self.group_events_by_client(valid_events)

        sorted_groups = self.sort_events_by_time(grouped_events)

        logger.info(f"Processing complete: {len(sorted_groups)} client groups created")

        return sorted_groups
