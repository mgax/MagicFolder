import time
import sys
from contextlib import contextmanager

codes = {
    'red': '\033[91m',
    'green': '\033[92m',
    'yellow': '\033[93m',
    'default': '\033[0m',
}

class ColorfulUi(object):
    def out(self, text):
        sys.stdout.write(text)
        sys.stdout.flush()

    @contextmanager
    def colored(self, color):
        self.out(codes[color])
        yield self.out
        self.out(codes['default'])

    @contextmanager
    def status_line(self):
        class state: count = 0
        def clear():
            if state.count:
                self.out('\r' + ' ' * state.count + '\r')
                state.count = 0

        def line_printer(text):
            clear()
            assert len(text) < 80
            state.count = len(text)
            self.out(text)

        yield line_printer
        clear()

class DummyUi(ColorfulUi):
    def out(self, text):
        pass

def pretty_bytes(n):
    K = 1024
    if n < K:
        return "%d bytes" % n
    elif n < K ** 2:
        return "%.1f KiB" % (n / 1024.)
    elif n < K ** 3:
        return "%.1f MiB" % (n / (1024*1024.))
    else:
        return "%.1f GiB" % (n / (1024*1024*1024.))


def demo():
    cui = ColorfulUi()

    with cui.colored('red') as colored_text:
        colored_text('hello ')
    with cui.colored('green'):
        colored_text('world')
    cui.out('!\n')

    with cui.status_line() as write_line:
        write_line('asdf')
        time.sleep(.2)
        write_line('qwer')
        time.sleep(.2)
        write_line('lala')
        time.sleep(.2)

if __name__ == '__main__':
    demo()
