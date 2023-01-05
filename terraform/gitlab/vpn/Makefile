.PHONY: all
all:

include ../../../common.mk

SHELL = /bin/bash
easyrsa = $(abspath easy-rsa/easyrsa3/easyrsa)

client_cn = $(shell git config user.email)
cn = azul-gitlab-vpn
server_cn = $(cn)-server-$(AZUL_DEPLOYMENT_STAGE)
ca_cn = $(cn)-$(AZUL_DEPLOYMENT_STAGE)
pki = $(azul_easyrsa_pki)/gitlab/$(AZUL_DEPLOYMENT_STAGE)

export EASYRSA_CERT_EXPIRE = 365

.PHONY: _client
_client: check_env
	$(eval export EASYRSA_PKI = $(pki)/client)

.PHONY: _admin
_admin: check_env
	$(eval export EASYRSA_PKI = $(pki)/admin)

.PHONY: init
init: _client
	$(easyrsa) init-pki

.PHONY: request
request: _client
	EASYRSA_REQ_CN=$(client_cn) $(easyrsa) --batch gen-req $(client_cn) nopass
	@echo "Hold onto '$(EASYRSA_PKI)/reqs/pki/private/$(client_cn).key' and keep it confidential"
	@echo "Send the following certificate request to your administrator:"
	@echo
	@cat $(EASYRSA_PKI)/reqs/$(client_cn).req
	@echo

.PHONY: import
import: _client
	mkdir -p $(EASYRSA_PKI)/issued
	@echo "Paste the certificate you received from your administrator and hit Ctrl-D"
	@cat > $(EASYRSA_PKI)/issued/$(client_cn).crt
	@echo
	@echo "Run 'make config > ~/azul-gitlab-$(AZUL_DEPLOYMENT_STAGE)-$(client_cn).ovpn' to generate the OpenVPN configuration"
	@echo

.PHONY: config
config: _client
	@echo "Generating configuration for your OpenVPN client ..." >&2
	@if [ -t 1 ]; then echo "Standard output is a terminal." \
		"Please run this target as in 'make config > ~/azul-gitlab-$(AZUL_DEPLOYMENT_STAGE)-$(client_cn).ovpn'." >&2; false; fi
	@export endpoint=$$(aws ec2 describe-client-vpn-endpoints \
			--filters Name=tag:Name,Values=azul-gitlab \
			--query ClientVpnEndpoints[*].ClientVpnEndpointId \
			--output text) \
		&& aws ec2 export-client-vpn-client-configuration \
			--client-vpn-endpoint-id $$endpoint \
			--output=text
	@echo "pull-filter ignore redirect-gateway"
	@echo "<cert>"
	@cat $(EASYRSA_PKI)/issued/$(client_cn).crt | openssl x509 -inform pem
	@echo "</cert>"
	@echo "<key>"
	@cat $(EASYRSA_PKI)/private/$(client_cn).key
	@echo "</key>"
	@echo >&2
	@echo "... done. You can now import the generated configuration to your OpenVPN client." >&2
	@echo "On Ubuntu, this is done with" \
	      "'nmcli connection import type openvpn file ~/azul-gitlab-$(AZUL_DEPLOYMENT_STAGE)-$(client_cn).ovpn'" >&2
	@echo >&2
	@echo "Please make sure that other Internet traffic isn't routed through the VPN connection." >&2
	@echo "On Ubuntu, this is done with" \
	      "'nmcli connection modify azul-gitlab-$(AZUL_DEPLOYMENT_STAGE)-$(client_cn) ipv4.never-default true'" >&2
	@echo "For other Operating systems please refer to README.md section 9.1" >&2
	@echo >&2

.PHONY: ca
ca: _admin
	$(easyrsa) init-pki
	EASYRSA_REQ_CN=$(ca_cn) $(easyrsa) --batch build-ca nopass

.PHONY: server
server: _admin
	$(easyrsa) build-server-full $(server_cn) nopass

.PHONY: publish
publish: _admin
	aws acm import-certificate \
		--certificate fileb://$(EASYRSA_PKI)/issued/$(server_cn).crt \
		--private-key fileb://$(EASYRSA_PKI)/private/$(server_cn).key \
		--certificate-chain fileb://$(EASYRSA_PKI)/ca.crt
	@echo "Now run 'make apply' in parent directory"

_check/%: _admin
	@test ! -e $(EASYRSA_PKI)/issued/$*.crt \
		|| (echo Certificate $* already exists, you must must revoke it first. ; false)

import/%: _check/%
	@echo "Paste the certificate request you received from the client and hit Ctrl-D"
	@export request=$$(mktemp -u) \
		&& (cat > $$request) \
		&& rm -f $(EASYRSA_PKI)/reqs/$*.req \
		&& $(easyrsa) import-req $$request $* \
		&& rm $$request

sign/%: _check/%
	$(easyrsa) sign-req client $*
	@cat $(EASYRSA_PKI)/issued/$*.crt | openssl x509 -inform pem
	@echo

renew/%: _admin
	$(easyrsa) renew client $*
	@cat $(EASYRSA_PKI)/issued/$*.crt | openssl x509 -inform pem
	@echo

.PHONY: list
list: _admin
	@cat $(EASYRSA_PKI)/index.txt
	@echo

revoke/%: _admin
	$(easyrsa) revoke $*
	$(easyrsa) gen-crl

.PHONY: list_revocations
list_revocations: _admin
	$(easyrsa) gen-crl
	openssl crl -in $(EASYRSA_PKI)/crl.pem -noout -text
	@echo

.PHONY: publish_revocations
publish_revocations: _admin
	$(easyrsa) gen-crl
	@export endpoint_id=$$( \
		aws ec2 describe-client-vpn-endpoints \
			--filters Name=tag:Name,Values=azul-gitlab \
			--query "ClientVpnEndpoints[0].ClientVpnEndpointId" \
			--output text \
	) \
	&& aws ec2 import-client-vpn-client-certificate-revocation-list \
		--certificate-revocation-list file://$(EASYRSA_PKI)/crl.pem \
		--client-vpn-endpoint-id $${endpoint_id} \
		--region $${AWS_DEFAULT_REGION}

.PHONY: list_published_revocations
list_published_revocations: _admin
	@export endpoint_id=$$( \
		aws ec2 describe-client-vpn-endpoints \
			--filters Name=tag:Name,Values=azul-gitlab \
			--query "ClientVpnEndpoints[0].ClientVpnEndpointId" \
			--output text \
	) && ( \
		aws ec2 export-client-vpn-client-certificate-revocation-list \
			--client-vpn-endpoint-id $${endpoint_id} \
			--region $${AWS_DEFAULT_REGION} \
			--query CertificateRevocationList \
			--output text \
		| openssl crl -in /dev/stdin -noout -text \
	)

# Use this target in conjunction with either _admin or _client to run additional
# EasyRSA commands. 'eval "`make _admin _env`"' for example would configure an
# shell alias called 'easyrsa' pointing at the right executable and also set
# EASYRSA_PKI to point at the PKI directory containing the CA and server certs.

.PHONY: _env
_env:
	@echo "export EASYRSA_PKI='$(EASYRSA_PKI)'"
	@echo "alias 'easyrsa=$(easyrsa)'"


# Without this `make` would append a dot to $* in pattern rules
# https://stackoverflow.com/questions/4122831/disable-make-builtin-rules-and-variables-from-inside-the-make-file
.SUFFIXES:
