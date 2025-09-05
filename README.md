# sony-alpha-python

Simple incomplete library that lays out some of the bones required to connect to Sony Alpha cameras via python over their "Camera Control" PTP protocol and it's proprietary extensions. This is using the SSH authentication method, not the pairing method as SSH tunneling was far more reliable in my testing.

Handles connecting, and can also deal with zooming in and out. Might add more functions later, might also not :P

Originally made this for controlling my FX30's zoom with an Elgato Foot Pedal connected to a Raspberry Pi.

Thanks to AlphaFairy, Sony's documentation, and WireShark for helping sort out how the communication needed to happen.

Usage:
```python test_camera.py <CAMERA IP> <USER> <PASS>```

Requirements:
- A camera compatible with Sony's PTP 3 protocol (FX30, A6700 are examples)
- Connecting that camera to the same network as you're running this script from
- "Remote Shoot Function" enabled on said camera
- The authentication credentials from said camera
