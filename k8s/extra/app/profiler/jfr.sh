#!/busybox/sh

JAVA_TOOL_OPTIONS=""
filename=/app/profiler/out/veidemann-controller-$(date -Iseconds).jfr
/usr/lib/jvm/java-11-openjdk-amd64/bin/jcmd 1 JFR.start maxsize=1G  filename=${filename} dumponexit=true

#Syntax : JFR.start [options]
#
#Options: (options must be specified using the <key> or <key>=<value> syntax)
#        name : [optional] Name that can be used to identify recording, e.g. \"My Recording\" (STRING, no default value)
#        settings : [optional] Settings file(s), e.g. profile or default. See JRE_HOME/lib/jfr (STRING SET, no default value)
#        delay : [optional] Delay recording start with (s)econds, (m)inutes), (h)ours), or (d)ays, e.g. 5h. (NANOTIME, 0)
#        duration : [optional] Duration of recording in (s)econds, (m)inutes, (h)ours, or (d)ays, e.g. 300s. (NANOTIME, 0)
#        disk : [optional] Recording should be persisted to disk (BOOLEAN, no default value)
#        filename : [optional] Resulting recording filename, e.g. \"/home/user/My Recording.jfr\" (STRING, no default value)
#        maxage : [optional] Maximum time to keep recorded data (on disk) in (s)econds, (m)inutes, (h)ours, or (d)ays, e.g. 60m, or 0 for no limit (NANOTIME, 0)
#        maxsize : [optional] Maximum amount of bytes to keep (on disk) in (k)B, (M)B or (G)B, e.g. 500M, or 0 for no limit (MEMORY SIZE, 0)
#        dumponexit : [optional] Dump running recording when JVM shuts down (BOOLEAN, no default value)
#        path-to-gc-roots : [optional] Collect path to GC roots (BOOLEAN, false)
