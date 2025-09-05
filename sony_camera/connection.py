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
        
        # SSH tunnel
        self.ssh_process: Optional[subprocess.Popen] = None
        self.ssh_tunnel_active = False
        
        # Sockets
        self.command_socket: Optional[socket.socket] = None
        self.event_socket: Optional[socket.socket] = None
    
    async def establish_connection(self) -> tuple:
        """Establish connection (SSH tunnel if needed + sockets)"""
        # Determine target host
        if self.ssh_username and self.ssh_password:
            if not await self._establish_ssh_tunnel():
                raise Exception("SSH tunnel failed")
            target_host = 'localhost'
        else:
            target_host = self.ip_address
        
        # Create and connect sockets
        self.command_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.event_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # Set socket options
        self.command_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.event_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        
        # Connect both channels
        self.command_socket.connect((target_host, 15740))
        self.event_socket.connect((target_host, 15740))
        
        logging.info('📡 Both channels connected')
        
        return self.command_socket, self.event_socket
    
    async def _establish_ssh_tunnel(self) -> bool:
        """Establish SSH tunnel"""
        try:
            cmd = [
                'sshpass', '-p', self.ssh_password,
                'ssh', '-c', 'aes128-ctr', '-N', '-L', '15740:localhost:15740',
                '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null',
                '-o', 'PreferredAuthentications=keyboard-interactive',
                '-o', 'KbdInteractiveAuthentication=yes', '-o', 'PubkeyAuthentication=no',
                '-o', 'ConnectTimeout=10', f'{self.ssh_username}@{self.ip_address}'
            ]
            
            self.ssh_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            time.sleep(4)  # Give SSH time to establish
            
            if self.ssh_process.poll() is None:
                logging.info("✅ SSH tunnel established")
                self.ssh_tunnel_active = True
                return True
            else:
                logging.error("SSH tunnel failed")
                return False
                
        except Exception as e:
            logging.error(f"SSH tunnel error: {e}")
            return False
    
    async def disconnect(self):
        """Close all connections"""
        if self.command_socket:
            try:
                self.command_socket.close()
            except:
                pass
            self.command_socket = None
        
        if self.event_socket:
            try:
                self.event_socket.close()
            except:
                pass
            self.event_socket = None
        
        if self.ssh_tunnel_active and self.ssh_process:
            try:
                self.ssh_process.terminate()
                self.ssh_process.wait(timeout=5)
            except:
                pass
            self.ssh_process = None
            self.ssh_tunnel_active = False
        
        logging.info('Connection closed')
    
    def is_connected(self) -> bool:
        """Check if both sockets are connected"""
        return (self.command_socket is not None and 
                self.event_socket is not None)
