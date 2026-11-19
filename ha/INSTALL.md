# Install YAMLs

### Update main HomeAssistant

    cat <<EOF >>/opt/homeassistant/config/configuration.yaml
    
    homeassistant:
      packages: !include_dir_named packages
    EOF

### Install YAMLs

    mkdir -p /opt/homeassistant/config/packages
    cp homeassistant.yaml homeassistant_alerts.yaml /opt/homeassistant/config/packages
