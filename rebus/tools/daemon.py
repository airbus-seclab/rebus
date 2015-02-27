import os


def daemonize():
    if os.fork() != 0: # parent
        os._exit(0) # do not flush buffers

    os.umask(0)
    os.chdir("/")
    os.setsid()
    
    os.close(0)
    os.close(1)
    os.close(2)
    os.open("/dev/null",os.O_RDONLY) # Guaranteed to be
    os.open("/dev/null",os.O_WRONLY) # the lowest available
    os.open("/dev/null",os.O_WRONLY) # file descriptors

    if os.fork() != 0: # parent
        os._exit(0) # do not flush buffers
