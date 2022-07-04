import asyncio
import random
import time

import cvutils


async def task():
    name = cvutils.random_str(10)
    sleep = random.random() * 10
    print(f'[{name}] is sleeping for {cvutils.TerminalColors.OK_GREEN}{sleep}{cvutils.TerminalColors.END}')
    await asyncio.sleep(sleep)
    print(f'[{name}] is {cvutils.TerminalColors.FAIL}Done{cvutils.TerminalColors.END}')


if __name__ == '__main__':

    asyncio.get_event_loop().run_until_complete(asyncio.gather(*[task() for t in range(5)]))
