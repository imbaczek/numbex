from __future__ import absolute_import
import unittest
import datetime
import crypto

class TestSignatures(unittest.TestCase):
    def setUp(self):
        self.dsa, self.privkey, self.pubkey = crypto.generate_dsa_key_pair(1024)

    def test_basic_signature(self):
        v = ['+48581234', '+48581999', 'sip.freeconet.pl', 'freeconet', datetime.datetime.now()]
        sig = crypto.sign_record(self.dsa, *v)
        self.assert_(crypto.check_signature(self.dsa, sig, *v))

    def test_pubkey_signature(self):
        v = ['+48581234', '+48581999', 'sip.freeconet.pl', 'freeconet', datetime.datetime.now()]
        dsapub = crypto.parse_pub_key(self.pubkey)
        sig = crypto.sign_record(self.dsa, *v)
        self.assert_(crypto.check_signature(dsapub, sig, *v))



if __name__ == '__main__':
    unittest.main()
