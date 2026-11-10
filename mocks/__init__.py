"""Mock providers for the Larkfield integration hub: one FastAPI app, one router per provider.

The mocks never deduplicate anything. Every accepted request is recorded by its `reference`, so a
double delivery is visible as a count above one.
"""
