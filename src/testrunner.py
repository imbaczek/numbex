#!/usr/bin/python
import unittest
import logging

from tests.test_gitdb import *
from tests.test_crypto import *
from tests.test_database import *
from tests.test_utils import *
from tests.test_udp import *
from tests.test_soap import *
from tests.test_tracker import *

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()
