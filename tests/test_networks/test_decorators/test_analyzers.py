"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    classname,
)
from minos.networks import (
    BrokerCommandEnrouteDecorator,
    BrokerEventEnrouteDecorator,
    BrokerQueryEnrouteDecorator,
    EnrouteAnalyzer,
    RestCommandEnrouteDecorator,
    RestQueryEnrouteDecorator,
)
from tests.utils import (
    FakeService,
)


class TestEnrouteAnalyzer(unittest.IsolatedAsyncioTestCase):
    def test_decorated_str(self):
        analyzer = EnrouteAnalyzer(classname(FakeService))
        self.assertEqual(FakeService, analyzer.decorated)

    def test_multiple_decorators(self):
        analyzer = EnrouteAnalyzer(FakeService)

        observed = analyzer.get_all()
        expected = {
            "get_tickets": {BrokerQueryEnrouteDecorator("GetTickets"), RestQueryEnrouteDecorator("tickets/", "GET")},
            "create_ticket": {
                BrokerCommandEnrouteDecorator("CreateTicket"),
                BrokerCommandEnrouteDecorator("AddTicket"),
                RestCommandEnrouteDecorator("orders/", "GET"),
            },
            "ticket_added": {BrokerEventEnrouteDecorator("TicketAdded")},
        }

        self.assertEqual(expected, observed)

    def test_get_only_rest_decorators(self):
        analyzer = EnrouteAnalyzer(FakeService)

        observed = analyzer.get_rest_command_query()
        expected = {
            "get_tickets": {RestQueryEnrouteDecorator("tickets/", "GET")},
            "create_ticket": {RestCommandEnrouteDecorator("orders/", "GET")},
        }

        self.assertEqual(expected, observed)

    def test_get_only_command_decorators(self):
        analyzer = EnrouteAnalyzer(FakeService)

        observed = analyzer.get_broker_command_query()
        expected = {
            "get_tickets": {BrokerQueryEnrouteDecorator("GetTickets")},
            "create_ticket": {
                BrokerCommandEnrouteDecorator("CreateTicket"),
                BrokerCommandEnrouteDecorator("AddTicket"),
            },
        }

        self.assertEqual(expected, observed)

    def test_get_only_event_decorators(self):
        analyzer = EnrouteAnalyzer(FakeService)

        observed = analyzer.get_broker_event()
        expected = {"ticket_added": {BrokerEventEnrouteDecorator("TicketAdded")}}

        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()