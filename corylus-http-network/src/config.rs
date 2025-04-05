use std::net::IpAddr;

#[derive(Copy, Clone)]
pub struct NetworkConfig {
    port: u16,
    port_count: u16,
    net_interface: IpAddr,
    mask: u8,
    outbound_net_interface: IpAddr,
}

impl NetworkConfig {
    pub fn new(
        port: u16,
        port_count: u16,
        net_interface: IpAddr,
        mask: u8,
        outbound_net_interface: IpAddr,
    ) -> Self {
        Self {
            port,
            port_count,
            net_interface,
            mask,
            outbound_net_interface,
        }
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn port_count(&self) -> u16 {
        self.port_count
    }

    pub fn net_interface(&self) -> IpAddr {
        self.net_interface
    }

    pub fn mask(&self) -> u8 {
        self.mask
    }

    pub fn outbound_net_interface(&self) -> IpAddr {
        self.outbound_net_interface
    }
}
