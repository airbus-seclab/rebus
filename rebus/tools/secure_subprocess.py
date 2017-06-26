import subprocess
import os
import distutils.spawn
import logging

NOACCESS = 0
TMP = 1  # Writable access to system wide /tmp
UNIX = 1 << 1  # Unix sockets
FS = 1 << 2  # full filesystem

log = logging.getLogger("rebus.secure_subprocess")

firejail = distutils.spawn.find_executable("firejail")
secure = []
if firejail is None:
    ch = logging.StreamHandler()
    log = logging.getLogger("rebus.secure_subprocess.initialization")
    log.addHandler(ch)
    log.warning("firejail binary not found, using INSECURE non sandboxed mode!")
else:
    secure.append(firejail)
    # TODO add private-etc
    secure.extend(
        ["--noprofile", "--force", "--quiet", "--nosound", "--private-dev",
         "--env=TMPDIR="+os.environ['HOME'], "--private", "--seccomp", ])


def make_firejail_cmdline(flags, cmd):
    if secure == []:
        return cmd

    if flags is None:
        flags = 0

    secres = list(secure)

    # TODO : check if binary is in homedir, in that case
    # add it to --private-home

    if flags & FS == FS:
        # TODO: find a way to specify specific files
        secres.remove("--private")
    elif flags & TMP == TMP:
        # TODO be more restrictive
        pass
    else:
        secres.append("--read-only=/*")

    if flags & UNIX == UNIX:
        secres.append("--net=unix")
    else:
        secres.append("--net=none")

    full_cmd = secres + ["--"] + cmd

    return full_cmd


def check_output(cmd, flags=NOACCESS, *args, **kwargs):
    full_cmd = make_firejail_cmdline(flags, cmd)
    return subprocess.check_output(full_cmd, *args, **kwargs)


def Popen(cmd, flags=NOACCESS, *args, **kwargs):
    full_cmd = make_firejail_cmdline(flags, cmd)
    return subprocess.Popen(full_cmd, *args, **kwargs)
