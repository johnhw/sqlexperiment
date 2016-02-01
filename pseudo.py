import random
import hashlib, base64

alphabet = 'ABCDEFGHIKLMNOPRSTUVWZ'
vowels = "UEIOA"
consonants = "".join(set(alphabet) - set(vowels))

def pron_encode(hx, off=0):
    """Encode a hex string as a pronouncable letter sequence. 
    If off=0, starts with a consonant, off=1 to start with a vowel"""
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
    """Create a pronouncable hash of a string"""
    sha = hashlib.sha256()
    sha.update(user_id)   
    return pron_encode(sha.hexdigest(),off=off)[:n]

def verify_pseudo(user_id, check=3):
    """Verify that the pseudonym is valid (i.e. not mistyped)"""
    uid =  user_id.replace("-","")    
    n = len(uid) - check    
    return check_pseudo(uid[:-check], n=check, off=n%2) == uid[-check:]

def get_pseudo(n=7, split=5, check=3):
    """Return a pseudonym of the form ABC-DE, where ABC are random letters, and DE are check
    digits"""    
    uid = random.getrandbits(256)
    user_id = check_pseudo("%x"%uid, n=n)        
    check = check_pseudo(user_id, n=check, off=n%2)    
    pseudo = user_id + check
    return "-".join([pseudo[i:i+split] for i in range(0, len(pseudo), split)])

if __name__=="__main__":
    print get_pseudo(n=7,check=3), verify_pseudo(get_pseudo())
    