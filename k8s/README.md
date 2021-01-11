Java Mission Control (JMC) on fedora

Article:
[JMC on fedora](https://fedoraproject.org/wiki/JMC_on_Fedora)

```bash
sudo dnf module install jmc:latest/default 
```

Java Components home page (JMC and JFR)
[Java Components](https://docs.oracle.com/javacomponents/)

https://dzone.com/articles/deep-dive-into-java-management-extensions-jmx
https://dzone.com/articles/remote-debugging-java-applications-with-jdwp

https://github.com/prometheus/jmx_exporter

[FAQ](https://community.oracle.com/tech/developers/discussion/2579717/jmc-frequently-asked-questions)
Q: Why do I get a lot fewer method samples then I expected?
A1: There's a bug in 7u60 (Linux only) that results in a large reduction of method samples. It's been fixed 7u71 and 8u20.
A2: By default, method samples are only taken at safepoints. Use -XX:UnlockDiagnosticVMOptions -XX:DebugNonSafepoints to enable some more method samples. (Will be default in later release)
A3: Flight Recorder does not sample threads that are running native code.

# Load the Java Debug Wire Protocol (JDWP) library:
              # - listen for the socket connection on port 5005
              # - not suspending the JVM before the main class loads
              # Since controller is only process in JVM we can be more generous:
              # JVM max heap size is 1/2 of memory limit
              # JVM min heap size is 1/4 of memory limit
              #
              # Async profiler
              # see https://github.com/jvm-profiling-tools/async-profiler
              # and https://hackernoon.com/profiling-java-applications-with-async-profiler-049s2790
              #
              # Java Flight Recorder
              # see https://docs.oracle.com/javacomponents/jmc-5-4/jfr-runtime-guide/run.htm#JFRUH164

              value: -XX:MinRAMPercentage=25.0 -XX:MaxRAMPercentage=75.0
                -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints
                -Dcom.sun.management.jmxremote
                -Dcom.sun.management.jmxremote.port=9010
                -Dcom.sun.management.jmxremote.ssl=false
                -Dcom.sun.management.jmxremote.authenticate=false
                # -XX:+FlightRecorder
                # -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005
                # Remote debugging
                # -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints
                # -Dcom.sun.management.jmxremote.local.only=false
                # -Dcom.sun.management.jmxremote.ssl=false
                # -Dcom.sun.management.jmxremote.authenticate=false
                # -Dcom.sun.management.jmxremote.port=9010
                # -Dcom.sun.management.jmxremote.rmi.port=9010
                # -Djava.rmi.server.hostname=localhost
                # -XX:FlightRecorderOptions=dumponexit=true,dumponexitpath=rec.jfr
                # -XX:StartFlightRecording=duration=60s,filename=/app/profiler/out/recording.jfr
              # -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+UseSerialGC -agentpath:"/app/profiler/async-profiler-1.8.3-linux-x64/build/libasyncProfiler.so=start,event=wall,file=/app/profiler/out/wall-flame-graph.svg,threads,svg,simple,title=Wall clock profile,width=1920"
  
