import multiprocessing as mp


def _calc_sq(x):

    return x**2

def calc_sq(x):    
    pool = mp.Pool(8)
    res = pool.map( _calc_sq, x )


    return res