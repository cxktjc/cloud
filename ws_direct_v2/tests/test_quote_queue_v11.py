"""Priority may overtake unrelated far quotes, never a symbol's predecessors."""
import queue
import threading
import unittest
from types import SimpleNamespace

from v9711_live.execution.quote_queue import QuoteQueue


def tick(symbol, price, number=0, source='tick'):
    return SimpleNamespace(symbol=symbol, price=str(price), valid=True, number=number, source=source)


class QuoteQueueTests(unittest.TestCase):
    def test_batch_takes_only_distinct_near_heads_and_leaves_rebound_and_wake_in_order(self):
        events = self.make()
        a, b, rebound = tick('A', .99), tick('B', .98), tick('B', 1.1)
        for item in (a, b, rebound, None):
            events.put(item)
        self.assertIs(events.take_distinct_candidate(set()), a)
        self.assertIs(events.take_distinct_candidate({'A'}), b)
        self.assertIsNone(events.take_distinct_candidate({'A', 'B'}))
        events.task_done()
        events.task_done()
        self.assertEqual(self.drain(events), [None, rebound])

    def test_batch_cannot_skip_a_protected_update_or_a_far_predecessor(self):
        events = self.make(watched={'A'})
        far, near = tick('A', 1.1), tick('B', .99)
        events.put(far)
        events.put(near)
        self.assertIsNone(events.take_distinct_candidate({'A'}))
        self.assertEqual(self.drain(events), [far, near])
        events.put(far)
        events.put(tick('A', .99))
        self.assertIsNone(events.take_distinct_candidate(set()))

    def make(self, symbols=('A', 'B'), watched=()):
        result = QuoteQueue(8192)
        result.set_policy({s: dict(limit_price=1.) for s in symbols}, watched)
        return result

    def drain(self, events):
        result = []
        while not events.empty():
            result.append(events.get_nowait())
            events.task_done()
        events.join()
        return result

    def test_unconfigured_queue_retains_global_fifo(self):
        events = QuoteQueue()
        values = [tick('A', 1.1), None, tick('B', 1.01)]
        for item in values:
            events.put(item)
        self.assertEqual(self.drain(events), values)

    def test_qualifying_symbol_overtakes_thousands_of_unrelated_far_quotes_without_loss(self):
        events = self.make()
        old = [tick('A', 1.1, i) for i in range(3000)]
        for item in old:
            events.put(item)
        opportunity = tick('B', .99)
        events.put(opportunity)
        self.assertIs(events.get_nowait(), opportunity)
        events.task_done()
        self.assertEqual(self.drain(events), old)

    def test_cross_source_a_b_a_keeps_all_predecessors(self):
        events = self.make()
        values = [tick('A', 1.1, 1), tick('A', .99, 2, 'bid0'), tick('A', 1.1, 3)]
        for item in values:
            events.put(item)
        self.assertEqual(self.drain(events), values)

    def test_earlier_crossing_wins_over_later_crossing_with_far_predecessors(self):
        events = self.make()
        far = tick('A', 1.1)
        earlier, later = tick('B', .99), tick('A', .98)
        for item in (far, earlier, later):
            events.put(item)
        self.assertEqual(self.drain(events), [earlier, far, later])

    def test_watched_incumbent_rebound_keeps_priority_before_a_new_challenger(self):
        events = self.make(watched={'A'})
        rebound, challenger = tick('A', 1.1), tick('B', .99)
        events.put(rebound)
        events.put(challenger)
        self.assertEqual(self.drain(events), [rebound, challenger])

    def test_policy_update_reclassifies_quotes_already_queued(self):
        events = self.make()
        far, newly_eligible = tick('A', 2), tick('B', 1.1)
        events.put(far)
        events.put(newly_eligible)
        events.set_policy({'A': dict(limit_price=1), 'B': dict(limit_price=1.1)}, ())
        self.assertEqual(self.drain(events), [newly_eligible, far])

    def test_watch_change_promotes_an_already_queued_incumbent(self):
        events = self.make()
        a, b = tick('A', 1.1), tick('B', 1.1)
        events.put(a)
        events.put(b)
        events.watch('B', True)
        self.assertEqual(self.drain(events), [b, a])

    def test_background_is_not_starved_and_capacity_remains_bounded(self):
        events = self.make()
        far = tick('A', 1.1)
        events.put(far)
        for i in range(32):
            events.put(tick('B', .99, i))
        values = self.drain(events)
        self.assertLessEqual(values.index(far), 16)
        bounded = QuoteQueue(1)
        bounded.put(None)
        with self.assertRaises(queue.Full):
            bounded.put_nowait(None)

    def test_concurrent_producers_preserve_each_symbols_entire_sequence(self):
        events = self.make()
        def produce(symbol):
            for number in range(1000):
                events.put(tick(symbol, .99 if number % 2 else 1.1, number))
        producers = [threading.Thread(target=produce, args=(s,)) for s in ('A', 'B')]
        for producer in producers:
            producer.start()
        for producer in producers:
            producer.join(2)
            self.assertFalse(producer.is_alive())
        values = self.drain(events)
        self.assertEqual(len(values), 2000)
        for symbol in ('A', 'B'):
            self.assertEqual([item.number for item in values if item.symbol == symbol], list(range(1000)))
