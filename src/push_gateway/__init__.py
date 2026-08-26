"""Personal Agent push gateway: relays tiny wake frames from instances to FCM."""

#: CalVer, and the SAME value the release tag carries. The two had drifted apart -- releases
#: were tagged v2026.8.26 while this said 0.1.0 -- which mattered once the gateway started
#: reporting push_gateway_build_info{version=...}: the metric named a version no image had.
__version__ = "2026.8.27"
