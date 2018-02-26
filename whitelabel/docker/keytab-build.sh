#!/usr/bin/env bash


# Prestare attenzione che enctypes del keytab deve essere lo stesso presente nella configurazione del krb5.conf
# es.
# default_tkt_enctypes = arcfour-hmac
# default_tgs_enctypes = arcfour-hmac



ktutil
-> addent -password -p username@REALM -k 1 -e arcfour-hmac
-> wkt wasp2.keytab

# Per fare l'autenticazione ai servizi c'è bisogno che che gli hostname vegano risolti dal reverse dns