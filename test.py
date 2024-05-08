from app import InputModel, get_dataset
import fixture as fx


def test():
    print(get_dataset(InputModel(**fx.input1)) == str(fx.output1))
    print(get_dataset(InputModel(**fx.input2)) == str(fx.output2))
    print(get_dataset(InputModel(**fx.input3)) == str(fx.output3))


if __name__ == "__main__":
    test()
