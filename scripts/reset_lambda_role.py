"""
Attempt to fix KMSAccessDeniedException when invoking a function.

See Troubleshooting section in README.md for details.
"""
from azul.lambdas import (
    LambdaFunctions,
)


def main():
    functions = LambdaFunctions()
    functions.reset_lambda_roles()


if __name__ == '__main__':
    main()
