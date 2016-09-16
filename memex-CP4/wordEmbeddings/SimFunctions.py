import math

class SimFunctions:
    """
    Use this as the class for implementating vector similarity functions
    """

    @staticmethod
    def abs_dot_product_sim(vector1, vector2):
        if len(vector1) != len(vector2):
            raise Exception
        sim = 0.0
        for i in range(0, len(vector1)):
            sim += (vector1[i]*vector2[i])
        return math.fabs(sim)

    @staticmethod
    def abs_cosine_sim(vector1, vector2):
        if len(vector1) != len(vector2):
            raise Exception
        total1 = 0.0
        total2 = 0.0
        sim = 0.0
        for i in range(0, len(vector1)):
            sim += (vector1[i]*vector2[i])
            total1 += (vector1[i]*vector1[i])
            total2 += (vector2[i]*vector2[i])
        total1 = math.sqrt(total1)
        total2 = math.sqrt(total2)
        if total1 == 0.0 or total2 == 0.0:
            # print 'divide by zero problem. Returning 0.0'
            # print vector1
            # print vector2
            return 0.0
        else:
            return math.fabs(sim/(total1*total2))
