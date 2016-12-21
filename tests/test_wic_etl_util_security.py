import context
from wic.etl.util.security import *
import inspect

def test_AESCipher_init_NoKey():
    assert AESCipher().__class__ is AESCipher

def test_AESCipher_init_CipherKey():
    assert AESCipher(cipher_key).__class__ is AESCipher

"""
def test_AESCipher_encrypt():
    c = AESCipher(cipher_key)
    assert 'hello,world' == c.decrypt(c.encrypt('hello,world'))
"""
