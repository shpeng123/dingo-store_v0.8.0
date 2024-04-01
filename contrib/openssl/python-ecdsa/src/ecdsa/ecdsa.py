#! /usr/bin/env python

"""
Implementation of Elliptic-Curve Digital Signatures.

NOTE: This a low level implementation of ECDSA, for normal applications
you should be looking at the keys.py module.

Classes and methods for elliptic-curve signatures:
private keys, public keys, signatures,
NIST prime-modulus curves with modulus lengths of
192, 224, 256, 384, and 521 bits.

Example:

  # (In real-life applications, you would probably want to
  # protect against defects in SystemRandom.)
  from random import SystemRandom
  randrange = SystemRandom().randrange

  # Generate a public/private key pair using the NIST Curve P-192:

  g = generator_192
  n = g.order()
  secret = randrange( 1, n )
  pubkey = Public_key( g, g * secret )
  privkey = Private_key( pubkey, secret )

  # Signing a hash value:

  hash = randrange( 1, n )
  signature = privkey.sign( hash, randrange( 1, n ) )

  # Verifying a signature for a hash value:

  if pubkey.verifies( hash, signature ):
    print_("Demo verification succeeded.")
  else:
    print_("*** Demo verification failed.")

  # Verification fails if the hash value is modified:

  if pubkey.verifies( hash-1, signature ):
    print_("**** Demo verification failed to reject tampered hash.")
  else:
    print_("Demo verification correctly rejected tampered hash.")

Version of 2009.05.16.

Revision history:
      2005.12.31 - Initial version.
      2008.11.25 - Substantial revisions introducing new classes.
      2009.05.16 - Warn against using random.randrange in real applications.
      2009.05.17 - Use random.SystemRandom by default.

Written in 2005 by Peter Pearson and placed in the public domain.
"""

from six import int2byte, b
from . import ellipticcurve
from . import numbertheory
from .util import bit_length
from ._compat import remove_whitespace


class RSZeroError(RuntimeError):
    pass


class InvalidPointError(RuntimeError):
    pass


class Signature(object):
    """ECDSA signature."""

    def __init__(self, r, s):
        self.r = r
        self.s = s

    def recover_public_keys(self, hash, generator):
        """Returns two public keys for which the signature is valid
        hash is signed hash
        generator is the used generator of the signature
        """
        curve = generator.curve()
        n = generator.order()
        r = self.r
        s = self.s
        e = hash
        x = r

        # Compute the curve point with x as x-coordinate
        alpha = (
            pow(x, 3, curve.p()) + (curve.a() * x) + curve.b()
        ) % curve.p()
        beta = numbertheory.square_root_mod_prime(alpha, curve.p())
        y = beta if beta % 2 == 0 else curve.p() - beta

        # Compute the public key
        R1 = ellipticcurve.PointJacobi(curve, x, y, 1, n)
        Q1 = numbertheory.inverse_mod(r, n) * (s * R1 + (-e % n) * generator)
        Pk1 = Public_key(generator, Q1)

        # And the second solution
        R2 = ellipticcurve.PointJacobi(curve, x, -y, 1, n)
        Q2 = numbertheory.inverse_mod(r, n) * (s * R2 + (-e % n) * generator)
        Pk2 = Public_key(generator, Q2)

        return [Pk1, Pk2]


class Public_key(object):
    """Public key for ECDSA."""

    def __init__(self, generator, point, verify=True):
        """Low level ECDSA public key object.

        :param generator: the Point that generates the group (the base point)
        :param point: the Point that defines the public key
        :param bool verify: if True check if point is valid point on curve

        :raises InvalidPointError: if the point parameters are invalid or
            point does not lay on the curve
        """

        self.curve = generator.curve()
        self.generator = generator
        self.point = point
        n = generator.order()
        p = self.curve.p()
        if not (0 <= point.x() < p) or not (0 <= point.y() < p):
            raise InvalidPointError(
                "The public point has x or y out of range."
            )
        if verify and not self.curve.contains_point(point.x(), point.y()):
            raise InvalidPointError("Point does not lay on the curve")
        if not n:
            raise InvalidPointError("Generator point must have order.")
        # for curve parameters with base point with cofactor 1, all points
        # that are on the curve are scalar multiples of the base point, so
        # verifying that is not necessary. See Section 3.2.2.1 of SEC 1 v2
        if (
            verify
            and self.curve.cofactor() != 1
            and not n * point == ellipticcurve.INFINITY
        ):
            raise InvalidPointError("Generator point order is bad.")

    def __eq__(self, other):
        """Return True if the keys are identical, False otherwise.

        Note: for comparison, only placement on the same curve and point
        equality is considered, use of the same generator point is not
        considered.
        """
        if isinstance(other, Public_key):
            return self.curve == other.curve and self.point == other.point
        return NotImplemented

    def __ne__(self, other):
        """Return False if the keys are identical, True otherwise."""
        return not self == other

    def verifies(self, hash, signature):
        """Verify that signature is a valid signature of hash.
        Return True if the signature is valid.
        """

        # From X9.62 J.3.1.

        G = self.generator
        n = G.order()
        r = signature.r
        s = signature.s
        if r < 1 or r > n - 1:
            return False
        if s < 1 or s > n - 1:
            return False
        c = numbertheory.inverse_mod(s, n)
        u1 = (hash * c) % n
        u2 = (r * c) % n
        if hasattr(G, "mul_add"):
            xy = G.mul_add(u1, self.point, u2)
        else:
            xy = u1 * G + u2 * self.point
        v = xy.x() % n
        return v == r


class Private_key(object):
    """Private key for ECDSA."""

    def __init__(self, public_key, secret_multiplier):
        """public_key is of class Public_key;
        secret_multiplier is a large integer.
        """

        self.public_key = public_key
        self.secret_multiplier = secret_multiplier

    def __eq__(self, other):
        """Return True if the points are identical, False otherwise."""
        if isinstance(other, Private_key):
            return (
                self.public_key == other.public_key
                and self.secret_multiplier == other.secret_multiplier
            )
        return NotImplemented

    def __ne__(self, other):
        """Return False if the points are identical, True otherwise."""
        return not self == other

    def sign(self, hash, random_k):
        """Return a signature for the provided hash, using the provided
        random nonce.  It is absolutely vital that random_k be an unpredictable
        number in the range [1, self.public_key.point.order()-1].  If
        an attacker can guess random_k, he can compute our private key from a
        single signature.  Also, if an attacker knows a few high-order
        bits (or a few low-order bits) of random_k, he can compute our private
        key from many signatures.  The generation of nonces with adequate
        cryptographic strength is very difficult and far beyond the scope
        of this comment.

        May raise RuntimeError, in which case retrying with a new
        random value k is in order.
        """

        G = self.public_key.generator
        n = G.order()
        k = random_k % n
        # Fix the bit-length of the random nonce,
        # so that it doesn't leak via timing.
        # This does not change that ks = k mod n
        ks = k + n
        kt = ks + n
        if bit_length(ks) == bit_length(n):
            p1 = kt * G
        else:
            p1 = ks * G
        r = p1.x() % n
        if r == 0:
            raise RSZeroError("amazingly unlucky random number r")
        s = (
            numbertheory.inverse_mod(k, n)
            * (hash + (self.secret_multiplier * r) % n)
        ) % n
        if s == 0:
            raise RSZeroError("amazingly unlucky random number s")
        return Signature(r, s)


def int_to_string(x):
    """Convert integer x into a string of bytes, as per X9.62."""
    assert x >= 0
    if x == 0:
        return b("\0")
    result = []
    while x:
        ordinal = x & 0xFF
        result.append(int2byte(ordinal))
        x >>= 8

    result.reverse()
    return b("").join(result)


def string_to_int(s):
    """Convert a string of bytes into an integer, as per X9.62."""
    result = 0
    for c in s:
        if not isinstance(c, int):
            c = ord(c)
        result = 256 * result + c
    return result


def digest_integer(m):
    """Convert an integer into a string of bytes, compute
     its SHA-1 hash, and convert the result to an integer."""
    #
    # I don't expect this function to be used much. I wrote
    # it in order to be able to duplicate the examples
    # in ECDSAVS.
    #
    from hashlib import sha1

    return string_to_int(sha1(int_to_string(m)).digest())


def point_is_valid(generator, x, y):
    """Is (x,y) a valid public key based on the specified generator?"""

    # These are the tests specified in X9.62.

    n = generator.order()
    curve = generator.curve()
    p = curve.p()
    if not (0 <= x < p) or not (0 <= y < p):
        return False
    if not curve.contains_point(x, y):
        return False
    if (
        curve.cofactor() != 1
        and not n * ellipticcurve.PointJacobi(curve, x, y, 1)
        == ellipticcurve.INFINITY
    ):
        return False
    return True


# secp112r1 curve
_p = int(remove_whitespace("DB7C 2ABF62E3 5E668076 BEAD208B"), 16)
# s = 00F50B02 8E4D696E 67687561 51752904 72783FB1
_a = int(remove_whitespace("DB7C 2ABF62E3 5E668076 BEAD2088"), 16)
_b = int(remove_whitespace("659E F8BA0439 16EEDE89 11702B22"), 16)
_Gx = int(remove_whitespace("09487239 995A5EE7 6B55F9C2 F098"), 16)
_Gy = int(remove_whitespace("A89C E5AF8724 C0A23E0E 0FF77500"), 16)
_r = int(remove_whitespace("DB7C 2ABF62E3 5E7628DF AC6561C5"), 16)
_h = 1
curve_112r1 = ellipticcurve.CurveFp(_p, _a, _b, _h)
generator_112r1 = ellipticcurve.PointJacobi(
    curve_112r1, _Gx, _Gy, 1, _r, generator=True
)


# secp112r2 curve
_p = int(remove_whitespace("DB7C 2ABF62E3 5E668076 BEAD208B"), 16)
# s = 022757A1 114D69E 67687561 51755316 C05E0BD4
_a = int(remove_whitespace("6127 C24C05F3 8A0AAAF6 5C0EF02C"), 16)
_b = int(remove_whitespace("51DE F1815DB5 ED74FCC3 4C85D709"), 16)
_Gx = int(remove_whitespace("4BA30AB5 E892B4E1 649DD092 8643"), 16)
_Gy = int(remove_whitespace("ADCD 46F5882E 3747DEF3 6E956E97"), 16)
_r = int(remove_whitespace("36DF 0AAFD8B8 D7597CA1 0520D04B"), 16)
_h = 4
curve_112r2 = ellipticcurve.CurveFp(_p, _a, _b, _h)
generator_112r2 = ellipticcurve.PointJacobi(
    curve_112r2, _Gx, _Gy, 1, _r, generator=True
)


# secp128r1 curve
_p = int(remove_whitespace("FFFFFFFD FFFFFFFF FFFFFFFF FFFFFFFF"), 16)
# S = 000E0D4D 69E6768 75615175 0CC03A44 73D03679
# a and b are mod p, so a is equal to p-3, or simply -3
# _a = -3
_b = int(remove_whitespace("E87579C1 1079F43D D824993C 2CEE5ED3"), 16)
_Gx = int(remove_whitespace("161FF752 8B899B2D 0C28607C A52C5B86"), 16)
_Gy = int(remove_whitespace("CF5AC839 5BAFEB13 C02DA292 DDED7A83"), 16)
_r = int(remove_whitespace("FFFFFFFE 00000000 75A30D1B 9038A115"), 16)
_h = 1
curve_128r1 = ellipticcurve.CurveFp(_p, -3, _b, _h)
generator_128r1 = ellipticcurve.PointJacobi(
    curve_128r1, _Gx, _Gy, 1, _r, generator=True
)


# secp160r1
_p = int(remove_whitespace("FFFFFFFF FFFFFFFF FFFFFFFF FFFFFFFF 7FFFFFFF"), 16)
# S = 1053CDE4 2C14D696 E6768756 1517533B F3F83345
# a and b are mod p, so a is equal to p-3, or simply -3
# _a = -3
_b = int(remove_whitespace("1C97BEFC 54BD7A8B 65ACF89F 81D4D4AD C565FA45"), 16)
_Gx = int(
    remove_whitespace("4A96B568 8EF57328 46646989 68C38BB9 13CBFC82"), 16,
)
_Gy = int(
    remove_whitespace("23A62855 3168947D 59DCC912 04235137 7AC5FB32"), 16,
)
_r = int(
    remove_whitespace("01 00000000 00000000 0001F4C8 F927AED3 CA752257"), 16,
)
_h = 1
curve_160r1 = ellipticcurve.CurveFp(_p, -3, _b, _h)
generator_160r1 = ellipticcurve.PointJacobi(
    curve_160r1, _Gx, _Gy, 1, _r, generator=True
)


# NIST Curve P-192:
_p = 6277101735386680763835789423207666416083908700390324961279
_r = 6277101735386680763835789423176059013767194773182842284081
# s = 0x3045ae6fc8422f64ed579528d38120eae12196d5L
# c = 0x3099d2bbbfcb2538542dcd5fb078b6ef5f3d6fe2c745de65L
_b = int(
    remove_whitespace(
        """
    64210519 E59C80E7 0FA7E9AB 72243049 FEB8DEEC C146B9B1"""
    ),
    16,
)
_Gx = int(
    remove_whitespace(
        """
    188DA80E B03090F6 7CBF20EB 43A18800 F4FF0AFD 82FF1012"""
    ),
    16,
)
_Gy = int(
    remove_whitespace(
        """
    07192B95 FFC8DA78 631011ED 6B24CDD5 73F977A1 1E794811"""
    ),
    16,
)

curve_192 = ellipticcurve.CurveFp(_p, -3, _b, 1)
generator_192 = ellipticcurve.PointJacobi(
    curve_192, _Gx, _Gy, 1, _r, generator=True
)


# NIST Curve P-224:
_p = int(
    remove_whitespace(
        """
    2695994666715063979466701508701963067355791626002630814351
    0066298881"""
    )
)
_r = int(
    remove_whitespace(
        """
    2695994666715063979466701508701962594045780771442439172168
    2722368061"""
    )
)
# s = 0xbd71344799d5c7fcdc45b59fa3b9ab8f6a948bc5L
# c = 0x5b056c7e11dd68f40469ee7f3c7a7d74f7d121116506d031218291fbL
_b = int(
    remove_whitespace(
        """
    B4050A85 0C04B3AB F5413256 5044B0B7 D7BFD8BA 270B3943
    2355FFB4"""
    ),
    16,
)
_Gx = int(
    remove_whitespace(
        """
    B70E0CBD 6BB4BF7F 321390B9 4A03C1D3 56C21122 343280D6
    115C1D21"""
    ),
    16,
)
_Gy = int(
    remove_whitespace(
        """
    BD376388 B5F723FB 4C22DFE6 CD4375A0 5A074764 44D58199
    85007E34"""
    ),
    16,
)

curve_224 = ellipticcurve.CurveFp(_p, -3, _b, 1)
generator_224 = ellipticcurve.PointJacobi(
    curve_224, _Gx, _Gy, 1, _r, generator=True
)

# NIST Curve P-256:
_p = int(
    remove_whitespace(
        """
    1157920892103562487626974469494075735300861434152903141955
    33631308867097853951"""
    )
)
_r = int(
    remove_whitespace(
        """
    115792089210356248762697446949407573529996955224135760342
    422259061068512044369"""
    )
)
# s = 0xc49d360886e704936a6678e1139d26b7819f7e90L
# c = 0x7efba1662985be9403cb055c75d4f7e0ce8d84a9c5114abcaf3177680104fa0dL
_b = int(
    remove_whitespace(
        """
    5AC635D8 AA3A93E7 B3EBBD55 769886BC 651D06B0 CC53B0F6
    3BCE3C3E 27D2604B"""
    ),
    16,
)
_Gx = int(
    remove_whitespace(
        """
    6B17D1F2 E12C4247 F8BCE6E5 63A440F2 77037D81 2DEB33A0
    F4A13945 D898C296"""
    ),
    16,
)
_Gy = int(
    remove_whitespace(
        """
    4FE342E2 FE1A7F9B 8EE7EB4A 7C0F9E16 2BCE3357 6B315ECE
    CBB64068 37BF51F5"""
    ),
    16,
)

curve_256 = ellipticcurve.CurveFp(_p, -3, _b, 1)
generator_256 = ellipticcurve.PointJacobi(
    curve_256, _Gx, _Gy, 1, _r, generator=True
)

# NIST Curve P-384:
_p = int(
    remove_whitespace(
        """
    3940200619639447921227904010014361380507973927046544666794
    8293404245721771496870329047266088258938001861606973112319"""
    )
)
_r = int(
    remove_whitespace(
        """
    3940200619639447921227904010014361380507973927046544666794
    6905279627659399113263569398956308152294913554433653942643"""
    )
)
# s = 0xa335926aa319a27a1d00896a6773a4827acdac73L
# c = int(remove_whitespace(
#    """
#    79d1e655 f868f02f ff48dcde e14151dd b80643c1 406d0ca1
#    0dfe6fc5 2009540a 495e8042 ea5f744f 6e184667 cc722483"""
# ), 16)
_b = int(
    remove_whitespace(
        """
    B3312FA7 E23EE7E4 988E056B E3F82D19 181D9C6E FE814112
    0314088F 5013875A C656398D 8A2ED19D 2A85C8ED D3EC2AEF"""
    ),
    16,
)
_Gx = int(
    remove_whitespace(
        """
    AA87CA22 BE8B0537 8EB1C71E F320AD74 6E1D3B62 8BA79B98
    59F741E0 82542A38 5502F25D BF55296C 3A545E38 72760AB7"""
    ),
    16,
)
_Gy = int(
    remove_whitespace(
        """
    3617DE4A 96262C6F 5D9E98BF 9292DC29 F8F41DBD 289A147C
    E9DA3113 B5F0B8C0 0A60B1CE 1D7E819D 7A431D7C 90EA0E5F"""
    ),
    16,
)

curve_384 = ellipticcurve.CurveFp(_p, -3, _b, 1)
generator_384 = ellipticcurve.PointJacobi(
    curve_384, _Gx, _Gy, 1, _r, generator=True
)

# NIST Curve P-521:
_p = int(
    "686479766013060971498190079908139321726943530014330540939"
    "446345918554318339765605212255964066145455497729631139148"
    "0858037121987999716643812574028291115057151"
)
_r = int(
    "686479766013060971498190079908139321726943530014330540939"
    "446345918554318339765539424505774633321719753296399637136"
    "3321113864768612440380340372808892707005449"
)
# s = 0xd09e8800291cb85396cc6717393284aaa0da64baL
# c = int(remove_whitespace(
#    """
#         0b4 8bfa5f42 0a349495 39d2bdfc 264eeeeb 077688e4
#    4fbf0ad8 f6d0edb3 7bd6b533 28100051 8e19f1b9 ffbe0fe9
#    ed8a3c22 00b8f875 e523868c 70c1e5bf 55bad637"""
# ), 16)
_b = int(
    remove_whitespace(
        """
         051 953EB961 8E1C9A1F 929A21A0 B68540EE A2DA725B
    99B315F3 B8B48991 8EF109E1 56193951 EC7E937B 1652C0BD
    3BB1BF07 3573DF88 3D2C34F1 EF451FD4 6B503F00"""
    ),
    16,
)
_Gx = int(
    remove_whitespace(
        """
          C6 858E06B7 0404E9CD 9E3ECB66 2395B442 9C648139
    053FB521 F828AF60 6B4D3DBA A14B5E77 EFE75928 FE1DC127
    A2FFA8DE 3348B3C1 856A429B F97E7E31 C2E5BD66"""
    ),
    16,
)
_Gy = int(
    remove_whitespace(
        """
         118 39296A78 9A3BC004 5C8A5FB4 2C7D1BD9 98F54449
    579B4468 17AFBD17 273E662C 97EE7299 5EF42640 C550B901
    3FAD0761 353C7086 A272C240 88BE9476 9FD16650"""
    ),
    16,
)

curve_521 = ellipticcurve.CurveFp(_p, -3, _b, 1)
generator_521 = ellipticcurve.PointJacobi(
    curve_521, _Gx, _Gy, 1, _r, generator=True
)

# Certicom secp256-k1
_a = 0x0000000000000000000000000000000000000000000000000000000000000000
_b = 0x0000000000000000000000000000000000000000000000000000000000000007
_p = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F
_Gx = 0x79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798
_Gy = 0x483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8
_r = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141

curve_secp256k1 = ellipticcurve.CurveFp(_p, _a, _b, 1)
generator_secp256k1 = ellipticcurve.PointJacobi(
    curve_secp256k1, _Gx, _Gy, 1, _r, generator=True
)

# Brainpool P-160-r1
_a = 0x340E7BE2A280EB74E2BE61BADA745D97E8F7C300
_b = 0x1E589A8595423412134FAA2DBDEC95C8D8675E58
_p = 0xE95E4A5F737059DC60DFC7AD95B3D8139515620F
_Gx = 0xBED5AF16EA3F6A4F62938C4631EB5AF7BDBCDBC3
_Gy = 0x1667CB477A1A8EC338F94741669C976316DA6321
_q = 0xE95E4A5F737059DC60DF5991D45029409E60FC09

curve_brainpoolp160r1 = ellipticcurve.CurveFp(_p, _a, _b, 1)
generator_brainpoolp160r1 = ellipticcurve.PointJacobi(
    curve_brainpoolp160r1, _Gx, _Gy, 1, _q, generator=True
)

# Brainpool P-192-r1
_a = 0x6A91174076B1E0E19C39C031FE8685C1CAE040E5C69A28EF
_b = 0x469A28EF7C28CCA3DC721D044F4496BCCA7EF4146FBF25C9
_p = 0xC302F41D932A36CDA7A3463093D18DB78FCE476DE1A86297
_Gx = 0xC0A0647EAAB6A48753B033C56CB0F0900A2F5C4853375FD6
_Gy = 0x14B690866ABD5BB88B5F4828C1490002E6773FA2FA299B8F
_q = 0xC302F41D932A36CDA7A3462F9E9E916B5BE8F1029AC4ACC1

curve_brainpoolp192r1 = ellipticcurve.CurveFp(_p, _a, _b, 1)
generator_brainpoolp192r1 = ellipticcurve.PointJacobi(
    curve_brainpoolp192r1, _Gx, _Gy, 1, _q, generator=True
)

# Brainpool P-224-r1
_a = 0x68A5E62CA9CE6C1C299803A6C1530B514E182AD8B0042A59CAD29F43
_b = 0x2580F63CCFE44138870713B1A92369E33E2135D266DBB372386C400B
_p = 0xD7C134AA264366862A18302575D1D787B09F075797DA89F57EC8C0FF
_Gx = 0x0D9029AD2C7E5CF4340823B2A87DC68C9E4CE3174C1E6EFDEE12C07D
_Gy = 0x58AA56F772C0726F24C6B89E4ECDAC24354B9E99CAA3F6D3761402CD
_q = 0xD7C134AA264366862A18302575D0FB98D116BC4B6DDEBCA3A5A7939F

curve_brainpoolp224r1 = ellipticcurve.CurveFp(_p, _a, _b, 1)
generator_brainpoolp224r1 = ellipticcurve.PointJacobi(
    curve_brainpoolp224r1, _Gx, _Gy, 1, _q, generator=True
)

# Brainpool P-256-r1
_a = 0x7D5A0975FC2C3057EEF67530417AFFE7FB8055C126DC5C6CE94A4B44F330B5D9
_b = 0x26DC5C6CE94A4B44F330B5D9BBD77CBF958416295CF7E1CE6BCCDC18FF8C07B6
_p = 0xA9FB57DBA1EEA9BC3E660A909D838D726E3BF623D52620282013481D1F6E5377
_Gx = 0x8BD2AEB9CB7E57CB2C4B482FFC81B7AFB9DE27E1E3BD23C23A4453BD9ACE3262
_Gy = 0x547EF835C3DAC4FD97F8461A14611DC9C27745132DED8E545C1D54C72F046997
_q = 0xA9FB57DBA1EEA9BC3E660A909D838D718C397AA3B561A6F7901E0E82974856A7

curve_brainpoolp256r1 = ellipticcurve.CurveFp(_p, _a, _b, 1)
generator_brainpoolp256r1 = ellipticcurve.PointJacobi(
    curve_brainpoolp256r1, _Gx, _Gy, 1, _q, generator=True
)

# Brainpool P-320-r1
_a = int(
    remove_whitespace(
        """
    3EE30B568FBAB0F883CCEBD46D3F3BB8A2A73513F5EB79DA66190EB085FFA9
    F492F375A97D860EB4"""
    ),
    16,
)
_b = int(
    remove_whitespace(
        """
    520883949DFDBC42D3AD198640688A6FE13F41349554B49ACC31DCCD884539
    816F5EB4AC8FB1F1A6"""
    ),
    16,
)
_p = int(
    remove_whitespace(
        """
    D35E472036BC4FB7E13C785ED201E065F98FCFA6F6F40DEF4F92B9EC7893EC
    28FCD412B1F1B32E27"""
    ),
    16,
)
_Gx = int(
    remove_whitespace(
        """
    43BD7E9AFB53D8B85289BCC48EE5BFE6F20137D10A087EB6E7871E2A10A599
    C710AF8D0D39E20611"""
    ),
    16,
)
_Gy = int(
    remove_whitespace(
        """
    14FDD05545EC1CC8AB4093247F77275E0743FFED117182EAA9C77877AAAC6A
    C7D35245D1692E8EE1"""
    ),
    16,
)
_q = int(
    remove_whitespace(
        """
    D35E472036BC4FB7E13C785ED201E065F98FCFA5B68F12A32D482EC7EE8658
    E98691555B44C59311"""
    ),
    16,
)

curve_brainpoolp320r1 = ellipticcurve.CurveFp(_p, _a, _b, 1)
generator_brainpoolp320r1 = ellipticcurve.PointJacobi(
    curve_brainpoolp320r1, _Gx, _Gy, 1, _q, generator=True
)

# Brainpool P-384-r1
_a = int(
    remove_whitespace(
        """
    7BC382C63D8C150C3C72080ACE05AFA0C2BEA28E4FB22787139165EFBA91F9
    0F8AA5814A503AD4EB04A8C7DD22CE2826"""
    ),
    16,
)
_b = int(
    remove_whitespace(
        """
    04A8C7DD22CE28268B39B55416F0447C2FB77DE107DCD2A62E880EA53EEB62
    D57CB4390295DBC9943AB78696FA504C11"""
    ),
    16,
)
_p = int(
    remove_whitespace(
        """
    8CB91E82A3386D280F5D6F7E50E641DF152F7109ED5456B412B1DA197FB711
    23ACD3A729901D1A71874700133107EC53"""
    ),
    16,
)
_Gx = int(
    remove_whitespace(
        """
    1D1C64F068CF45FFA2A63A81B7C13F6B8847A3E77EF14FE3DB7FCAFE0CBD10
    E8E826E03436D646AAEF87B2E247D4AF1E"""
    ),
    16,
)
_Gy = int(
    remove_whitespace(
        """
    8ABE1D7520F9C2A45CB1EB8E95CFD55262B70B29FEEC5864E19C054FF991292
    80E4646217791811142820341263C5315"""
    ),
    16,
)
_q = int(
    remove_whitespace(
        """
    8CB91E82A3386D280F5D6F7E50E641DF152F7109ED5456B31F166E6CAC0425
    A7CF3AB6AF6B7FC3103B883202E9046565"""
    ),
    16,
)

curve_brainpoolp384r1 = ellipticcurve.CurveFp(_p, _a, _b, 1)
generator_brainpoolp384r1 = ellipticcurve.PointJacobi(
    curve_brainpoolp384r1, _Gx, _Gy, 1, _q, generator=True
)

# Brainpool P-512-r1
_a = int(
    remove_whitespace(
        """
    7830A3318B603B89E2327145AC234CC594CBDD8D3DF91610A83441CAEA9863
    BC2DED5D5AA8253AA10A2EF1C98B9AC8B57F1117A72BF2C7B9E7C1AC4D77FC94CA"""
    ),
    16,
)
_b = int(
    remove_whitespace(
        """
    3DF91610A83441CAEA9863BC2DED5D5AA8253AA10A2EF1C98B9AC8B57F1117
    A72BF2C7B9E7C1AC4D77FC94CADC083E67984050B75EBAE5DD2809BD638016F723"""
    ),
    16,
)
_p = int(
    remove_whitespace(
        """
    AADD9DB8DBE9C48B3FD4E6AE33C9FC07CB308DB3B3C9D20ED6639CCA703308
    717D4D9B009BC66842AECDA12AE6A380E62881FF2F2D82C68528AA6056583A48F3"""
    ),
    16,
)
_Gx = int(
    remove_whitespace(
        """
    81AEE4BDD82ED9645A21322E9C4C6A9385ED9F70B5D916C1B43B62EEF4D009
    8EFF3B1F78E2D0D48D50D1687B93B97D5F7C6D5047406A5E688B352209BCB9F822"""
    ),
    16,
)
_Gy = int(
    remove_whitespace(
        """
    7DDE385D566332ECC0EABFA9CF7822FDF209F70024A57B1AA000C55B881F81
    11B2DCDE494A5F485E5BCA4BD88A2763AED1CA2B2FA8F0540678CD1E0F3AD80892"""
    ),
    16,
)
_q = int(
    remove_whitespace(
        """
    AADD9DB8DBE9C48B3FD4E6AE33C9FC07CB308DB3B3C9D20ED6639CCA703308
    70553E5C414CA92619418661197FAC10471DB1D381085DDADDB58796829CA90069"""
    ),
    16,
)

curve_brainpoolp512r1 = ellipticcurve.CurveFp(_p, _a, _b, 1)
generator_brainpoolp512r1 = ellipticcurve.PointJacobi(
    curve_brainpoolp512r1, _Gx, _Gy, 1, _q, generator=True
)
