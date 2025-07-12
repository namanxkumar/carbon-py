import sys
import termios
import tty

from carbon import Data, Module, source


class TeleopCommand(Data):
    left: float
    right: float


class Teleop(Module):
    def __init__(self):
        super().__init__()

    @source(TeleopCommand)
    def teleop_command(self) -> TeleopCommand:
        print("Running Teleop (press 'q' to exit, use wasd for control)")

        def read_wasd_key():
            fd = sys.stdin.fileno()
            old_settings = termios.tcgetattr(fd)
            try:
                # Set cbreak mode and turn off echo
                tty.setcbreak(fd)
                new_settings = termios.tcgetattr(fd)
                new_settings[3] = new_settings[3] & ~termios.ECHO  # lflags
                termios.tcsetattr(fd, termios.TCSADRAIN, new_settings)
                ch = sys.stdin.read(1)
                print("\r\033[K", end="", flush=True)
                if ch == "w":
                    return TeleopCommand(left=1.0, right=1.0)
                elif ch == "s":
                    return TeleopCommand(left=-1.0, right=-1.0)
                elif ch == "d":
                    return TeleopCommand(left=0.5, right=1.0)
                elif ch == "a":
                    return TeleopCommand(left=1.0, right=0.5)
                elif ch == "q":
                    print("Exiting...")
                    sys.exit(0)
                return TeleopCommand(left=0.0, right=0.0)
            finally:
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

        return read_wasd_key()
