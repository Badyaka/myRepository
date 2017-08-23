'''
функция расчитывает последнюю цифру числа Фибоначи
'''
def fib_digit(n):
    fyb_lyst = [1, 1]
    i = 2
    if n <= 2:
        return 1
    else:
        while i <= n:
            fyb_lyst.append((fyb_lyst[i-2]+fyb_lyst[i-1]) % 10)
            i += 1
        return fyb_lyst[n-1]


def fib_mod(n, m):

    fyb_mod_lyst = [0 % m, 1 % m]
    i = 2
    if n > 2:
        while i <= n:
            fyb_mod_lyst.append((fyb_mod_lyst[i - 2] + fyb_mod_lyst[i - 1]) % m)
            i += 1

    return fyb_mod_lyst[n]


def main():
#    n = int(input())
#    print(fib_digit(n))
    n, m = map(int, input().split())
    print(fib_mod(n, m))

if __name__ == "__main__":
    main()

