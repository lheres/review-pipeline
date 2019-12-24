import io
from typing import Iterator, Optional

class StringIteratorIO(io.TextIOBase):

    def __init__(self, iter: Iterator[str]):
        self._iter = iter
        self._buff = ''

    def readable(self) -> bool:
        return True

    def _read1(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)

    def readline(self):
        line = []
        while True:
            i = self._buff.find('\n')
            if i == -1:
                line.append(self._buff)
                try:
                    self._buff = next(self._iter)
                except StopIteration:
                    self._buff = ''
                    break
            else:
                line.append(self._buff[:i+1])
                self._buff = self._buff[i+1:]
                break
        return ''.join(line)

def _str_iter():
    for a in range(100):
        yield str(a)*a

""" assert StringIteratorIO(_str_iter()).read() == ''.join(_str_iter())

for line in StringIteratorIO(_str_iter()):
    print(line)

f = StringIteratorIO(_str_iter())
print(f.read(5))
print(f.read(5))
print(f.read(5)) """