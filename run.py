import sys
import fetch_realtrade_info
from fetch_realtrade_info import lambda_function

fun_dict = {
    'fetch_realtrade_info': fetch_realtrade_info.lambda_function,
}

if __name__ == "__main__":
    if len(sys.argv) == 0:
        print('실행시킬 함수명을 입력해주세요.')

    fun_name = sys.argv[1]
    fun_dict[fun_name].lambda_handler(None, None)

