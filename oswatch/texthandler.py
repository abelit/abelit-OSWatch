class TextHandler(object):
    """docstring for TextHandler"""
    def format_text(self,text,*x):
        strings=text % (x)
        return strings
