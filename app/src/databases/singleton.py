

class Singleton(object):
    def __new__(cls, *args, **kw):
        """
        Class of singleton object

        :return: an instance of singleton object, useful in database memory update
        """  
        if not hasattr(cls, '_instance'):
            orig = super(Singleton, cls)
            cls._instance = orig.__new__(cls)
        return cls._instance