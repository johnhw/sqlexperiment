import random
import hashlib

alphabet = 'ABCDEFGHIKLMNOPRSTUVWZ'
vowels = "UEIOA"
consonants = "".join(set(alphabet) - set(vowels))

def pron_encode(hx, off=0):
    """Encode a hex string as a pronouncable letter sequence.

    Parameters:
        off: If off=0, starts with a consonant, off=1 to start with a vowel
        hx: Hex string to be encoded

    Returns:
        A pronouncable string (i.e alternating consonant/vowel)
    """
    i = int(hx, 16)
    j = off
    out = []
    lc = len(consonants)
    lv = len(vowels)
    while i>0:
        if j%2==0:
            out.append(consonants[i%lc])
            i = i // lc
        else:
            out.append(vowels[i%lv])
            i = i // lv
        j = j + 1
    return "".join(out)

def check_pseudo(user_id, n=3, off=0):
    """Create a pronouncable hash of a string.

    Parameters:
        user_id:    The id to check
        n:          Number of check digits to generate
        off:        Offset to be passed to pron_encode()

    Returns:
        check: Pronouncable check digit string (e.g. "GAR")

    """
    sha = hashlib.sha256()
    sha.update(user_id)
    return pron_encode(sha.hexdigest(),off=off)[:n]

def verify_pseudo(user_id, check=3):
    """Verify that the pseudonym is valid (i.e. not mistyped).

    Parameters:
        user_id:    pronouncable user ID to check
        check:      Number of check digits used (last n characters of the string)

    Returns:
        match: True if the check digits match, False otherwise

    """
    uid =  user_id.replace("-","")
    n = len(uid) - check
    return check_pseudo(uid[:-check], n=check, off=n%2) == uid[-check:]

def get_pseudo(n=7, split=5, check=3):
    """Return a pseudonym of the form ABC-DE, where ABC are random letters, and DE are check
    digits.


    Parameters:
        n:  Number of letters in the random pseudonym portion
        split: Interval to split string at with a "-" (e.g. 3 would produce strings like ABC-DEF-GHI)
        check:  Number of check digits to append

    Returns:
        pseudo: Pronounceable pseudonym


    """
    uid = random.getrandbits(256)
    user_id = check_pseudo("%x"%uid, n=n)
    check = check_pseudo(user_id, n=check, off=n%2)
    pseudo = user_id + check
    return "-".join([pseudo[i:i+split] for i in range(0, len(pseudo), split)])

if __name__=="__main__":
    print(get_pseudo(n=7,check=3), verify_pseudo(get_pseudo()))
