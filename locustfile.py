import os

import gevent
import locust

endpoints = [
    '/agencies',
    '/animators',
    '/cities',
    '/clientCards',
    '/clients',
    '/equipments',
    '/equipmentShops',
    '/events',
    '/orders',
    '/users',
]

# check

class UserPool(locust.FastHttpUser):
    @locust.task
    def flood_requests(self):
        def auth(endpoint):
            self.client.get(endpoint, auth=(os.environ["NAME"], os.environ["PASSWORD"]))
        pool = gevent.pool.Pool()
        for endpoint in endpoints:
            pool.spawn(auth, endpoint)
        pool.join()
