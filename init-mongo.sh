#!/bin/bash
set -e

# Attendre que MongoDB soit prêt
sleep 5

# Se connecter à MongoDB avec l'utilisateur root
mongosh --host localhost --port 27017 -u $MONGO_INITDB_ROOT_USERNAME -p $MONGO_INITDB_ROOT_PASSWORD --authenticationDatabase admin <<EOF
# Créer la base de données
use $MONGO_INITDB_DATABASE

# Créer un utilisateur pour cette base de données
db.createUser({
  user: 'bloguser',
  pwd: 'blogpassword',
  roles: [
    { role: 'readWrite', db: '$MONGO_INITDB_DATABASE' }
  ]
})

# Créer une collection 
db.createCollection("entreprises")                                                                         
print("Base de données $MONGO_INITDB_DATABASE initialisée avec succès")
EOF