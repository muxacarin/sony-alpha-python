"""
Sony Camera Connection Management - SSH Tunneling and Socket Handling
"""

import socket
import subprocess
import time
import logging
from typing import Optional

class ConnectionManager:
    """Handles SSH tunneling and socket connections"""
    
    def __init__(self, ip_address: str, ssh_username: str = None, ssh_password: str = None):
        self.ip_address = ip_address
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        
        # SSH tunnel - use dynamic port allocation to support multiple cameras
        self.ssh_process: Optional[subprocess.Popen] = None
        self.ssh_tunnel_active = False
        self.local_tunnel_port: Optional[int] = None  # Will be allocated dynamically
        
        # Sockets
        self.command_socket: Optional[socket.socket] = None
        self.event_socket: Optional[socket.socket] = None
    
    def _find_free_port(self) -> int:
        """Find a free local port for SSH tunnel"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))  # Bind to any free port
            s.listen(1)
            port = s.getsockname()[1]
        return port
    
    async def establish_connection(self) -> tuple:
        """Establish connection (SSH tunnel if needed + sockets)"""
        # Determine target host and port
        if self.ssh_username and self.ssh_password:
            if not await self._establish_ssh_tunnel():
                raise Exception("SSH tunnel failed")
            target_host = 'localhost'
            target_port = self.local_tunnel_port
        else:
            target_host = self.ip_address
            target_port = 15740  # Direct connection uses camera's PTP port
        
        # Create and connect sockets
        self.command_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.event_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # Set socket options
        self.command_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.event_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        
        # Connect both channels
        self.command_socket.connect((target_host, target_port))
        self.event_socket.connect((target_host, target_port))
        
        logging.info(f'📡 Both channels connected to {target_host}:{target_port}')
        
        return self.command_socket, self.event_socket
    
    async def _establish_ssh_tunnel(self) -> bool:
        """Establish SSH tunnel with dynamic port allocation"""
        try:
            # Allocate a free local port for this tunnel
            self.local_tunnel_port = self._find_free_port()
            tunnel_spec = f'{self.local_tunnel_port}:localhost:15740'
            
            logging.info(f"🔧 Creating SSH tunnel: localhost:{self.local_tunnel_port} → {self.ip_address}:15740")
            
            cmd = [
                'sshpass', '-p', self.ssh_password,
                'ssh', '-c', 'aes128-ctr', '-N', '-L', tunnel_spec,
                '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null',
                '-o', 'PreferredAuthentications=keyboard-interactive',
                '-o', 'KbdInteractiveAuthentication=yes', '-o', 'PubkeyAuthentication=no',
                '-o', 'ConnectTimeout=10', f'{self.ssh_username}@{self.ip_address}'
            ]
            
            self.ssh_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            time.sleep(4)  # Give SSH time to establish
            
            if self.ssh_process.poll() is None:
                logging.info(f"✅ SSH tunnel established on port {self.local_tunnel_port}")
                self.ssh_tunnel_active = True
                return True
            else:
                logging.error("SSH tunnel failed")
                return False
                
        except Exception as e:
            logging.error(f"SSH tunnel error: {e}")

    async def disconnect(self):
        """Close sockets and tear down SSH tunnel."""
        for name, sock in [('command', self.command_socket), ('event', self.event_socket)]:
            if sock:
                try:
                    sock.close()
                except Exception as e:
                    logging.debug(f"Error closing {name} socket: {e}")
        self.command_socket = None
        self.event_socket = None

        if self.ssh_process and self.ssh_process.poll() is None:
            try:
                self.ssh_process.terminate()
                self.ssh_process.wait(timeout=3)
            except Exception as e:
                logging.debug(f"Error terminating SSH tunnel: {e}")
                try:
                    self.ssh_process.kill()
                except Exception:
                    pass
        self.ssh_process = None
        self.ssh_tunnel_active = False