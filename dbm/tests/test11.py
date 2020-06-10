import datetime
import json

class P1():

    def __init__(self):
        print("Constructor de P1")

class P2():

    def __init__(self):
        print("Constructor de P2")
        self.variable = 1000

class P3(P1, P2):

    def __init__(self):
        P1.__init__(self)
        P2.__init__(self)

    def hola(self):
        print("hola")


if __name__ == "__main__":

    p3 = P3()
    print(p3.variable)


    pass
