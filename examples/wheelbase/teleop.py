import sys
import termios
import tty

from carbon import Module, producer
from carbon.common_data_types import Twist, Vector3


class Teleop(Module):
    def __init__(self):
        super().__init__()

    @producer(Twist)
    def teleop_command(self) -> Twist:
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
                    return Twist(
                        linear=Vector3(x=1.0, y=0.0, z=0.0),
                        angular=Vector3(x=0.0, y=0.0, z=0.0),
                    )
                elif ch == "s":
                    return Twist(
                        linear=Vector3(x=-1.0, y=0.0, z=0.0),
                        angular=Vector3(x=0.0, y=0.0, z=0.0),
                    )
                elif ch == "d":
                    return Twist(
                        linear=Vector3(x=0.5, y=0.0, z=0.0),
                        angular=Vector3(x=0.0, y=0.0, z=1.0),
                    )
                elif ch == "a":
                    return Twist(
                        linear=Vector3(x=0.5, y=0.0, z=0.0),
                        angular=Vector3(x=0.0, y=0.0, z=-1.0),
                    )
                elif ch == "q":
                    print("Exiting...")
                    sys.exit(0)
                return Twist(
                    linear=Vector3(x=0.0, y=0.0, z=0.0),
                    angular=Vector3(x=0.0, y=0.0, z=0.0),
                )
            finally:
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

        return read_wasd_key()
