import scrapy
import csv
from pathlib import Path
import re
import sys
import os

class KboSpider(scrapy.Spider):
    name = "kbo"
    url = "https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html?lang=fr"
    
    # Définition de la fonction qui va lancer les requêtes
    def start_requests(self):
        csv_file = Path(__file__).parent / "enterprise.csv"
        with open(csv_file, "r") as f:
            reader = csv.DictReader(f)
            # Limite pour récupérer les 10 premières entreprises, pour éviter le ban
            i = 0
            for row in reader:
                # Si la limite est atteinte, on quitte la boucle
                if i >= 10:
                    break
                # Récupération du numéro d'entreprise
                numero = row.get("EnterpriseNumber")
                if numero:
                    # Formatage du numéro de l'entreprise et ajout dans l'url
                    full_url = self.url + "&ondernemingsnummer=" + numero.replace('.', '')
                    yield scrapy.Request(url=full_url, callback=self.parse_page, meta={'numero': numero})
                    i += 1
    
    # Fonction qui va parser chaque page
    def parse_page(self, response):
        entreprise = {
            'numero': response.meta.get('numero'),
            'generalites': self.extraire_generalites(response),
            'fonctions': self.extraire_fonctions(response),
            'capacites': self.extraire_capacites(response),
            'qualites': self.extraire_qualites(response),
            'autorisations': self.extraire_autorisations(response),
            'nace_codes': {
                '2025': self.extraire_nace_codes(response, '2025'),
                '2008': self.extraire_nace_codes(response, '2008'),
                '2003': self.extraire_nace_codes(response, '2003')
            },
            'donnees_financieres': self.extraire_donnees_financieres(response),
            'liens_entites': self.extraire_liens_entites(response),
            'liens_externes': self.extraire_liens_externes(response)
        }
        yield entreprise
    
    # Extraction des généralités
    def extraire_generalites(self, response):
        generalites = {}
        
        # Extraction du numéro d'entreprise
        numero = response.xpath('//tr[td[contains(text(), "Numéro d\'entreprise")]]/td[2]/text()').get()
        if numero:
            generalites['numero'] = numero.strip()
        
        # Extraction du statut
        statut = response.xpath('//tr[td[contains(text(), "Statut")]]/td[2]//span/text()').get()
        if statut:
            generalites['statut'] = statut.strip()
        
        # Extraction de la situation juridique
        situation = response.xpath('//tr[td[contains(text(), "Situation juridique")]]/td[2]//span[1]/text()').get()
        if situation:
            generalites['situation_juridique'] = situation.strip()
        
        # Extraction de la date de début
        date_debut = response.xpath('//tr[td[contains(text(), "Date de début")]]/td[2]/text()').get()
        if date_debut:
            generalites['date_debut'] = date_debut.strip()
        
        # Extraction de la dénomination
        denomination = response.xpath('//tr[td[contains(text(), "Dénomination")]]/td[2]/text()[1]').get()
        if denomination:
            generalites['denomination'] = denomination.strip()
        
        # Extraction de l'adresse
        adresse = response.xpath('//tr[td[contains(text(), "Adresse du siège")]]/td[2]/text()').getall()
        if adresse:
            adresse_propre = ' '.join([a.strip() for a in adresse if a.strip()])
            generalites['adresse'] = adresse_propre
        
        # Extraction de la forme légale
        forme_legale = response.xpath('//tr[td[contains(text(), "Forme légale")]]/td[2]/text()[1]').get()
        if forme_legale:
            generalites['forme_legale'] = forme_legale.strip()
        
        return generalites
    
    def extraire_fonctions(self, response):
        fonctions = []
        
        # Parcourir toutes les lignes de la table des fonctions
        rows = response.xpath('//table[@id="toonfctie"]/tr')
        for row in rows:
            fonction = {}
            
            # Extraction du titre de la fonction
            titre = row.xpath('./td[1]/text()').get()
            if titre:
                fonction['titre'] = titre.strip()
            
            # Extraction du nom de la personne
            nom = row.xpath('./td[2]/text()').getall()
            if nom:
                # Suppression des espaces vides
                nom_propre = ' '.join([n.strip() for n in nom if n.strip()])
                fonction['nom'] = nom_propre
            
            # Extraction de la date de début de fonction
            date = row.xpath('./td[3]//span/text()').get()
            if date and 'Depuis le' in date:
                fonction['date_debut'] = date.replace('Depuis le', '').strip()
            
            if fonction and 'titre' in fonction and 'nom' in fonction:
                fonctions.append(fonction)
        
        return fonctions
    
    def extraire_capacites(self, response):
        capacites = []
        
        # Extraction des capacités entrepreneuriales
        section = response.xpath('//tr[td/h2[contains(text(), "Capacités entrepreneuriales")]]')
        if section:
            rows = section.xpath('./following-sibling::tr[position() < 3]')
            for row in rows:
                capacite = {}
                type_capacite = row.xpath('./td[1]/text()').get()
                if type_capacite:
                    capacite['type'] = type_capacite.strip()
                
                valeur = row.xpath('./td[2]/text()').get()
                if valeur:
                    capacite['valeur'] = valeur.strip()
                
                date = row.xpath('./td//span/text()').get()
                if date and 'Depuis le' in date:
                    capacite['date_debut'] = date.replace('Depuis le', '').strip()
                
                if capacite and 'type' in capacite and capacite['type']:
                    capacites.append(capacite)
        
        return capacites
    
    def extraire_qualites(self, response):
        qualites = []
        
        # Extraction des qualités
        section = response.xpath('//tr[td/h2[contains(text(), "Qualités")]]')
        if section:
            # Éviter de récupérer les autorisation par erreur
            next_section_path = '//tr[td/h2[contains(text(), "Autorisations")]]'
            next_section = response.xpath(next_section_path).get()
            
            # Si section Autorisations existe, on prend les lignes entre Qualités et Autorisations
            if next_section:
                # Trouver l'index des sections
                all_sections = response.xpath('//tr[td/h2]')
                qualites_index = -1
                autorisations_index = -1
                
                for i, section_row in enumerate(all_sections):
                    if "Qualités" in section_row.xpath('./td/h2/text()').get():
                        qualites_index = i
                    elif "Autorisations" in section_row.xpath('./td/h2/text()').get():
                        autorisations_index = i
                        break
                
                # Si on a trouvé les deux sections, on prend les lignes entre elles
                if qualites_index >= 0 and autorisations_index > qualites_index:
                    # Prendre les tr qui suivent directement la section Qualités
                    rows = response.xpath('//tr[td/h2[contains(text(), "Qualités")]]/following-sibling::tr')
                    
                    # Garder seulement les lignes jusqu'à la section Autorisations
                    ligne_count = 0
                    for row in rows:
                        # Arrêter si on atteint une autre section (h2)
                        if row.xpath('./td/h2').get():
                            break
                        
                        # Vérifier que ce n'est pas une ligne vide
                        text_content = row.xpath('./td/text()').get()
                        if not text_content or not text_content.strip():
                            continue
                        
                        qualite = {}
                        
                        # Extraire la description (et ignorer "Pas de données reprises dans la BCE.")
                        description = text_content.strip()
                        if description and "Pas de données reprises dans la BCE." not in description:
                            qualite['description'] = description
                        
                        # Extraire la date si présente
                        date = row.xpath('.//span[@class="upd"]/text()').get()
                        if date and 'Depuis le' in date:
                            qualite['date_debut'] = date.replace('Depuis le', '').strip()
                        
                        # Ajouter seulement si on a une description
                        if qualite and 'description' in qualite:
                            qualites.append(qualite)
                        
                        ligne_count += 1
                        # Limiter à 5 lignes pour éviter de déborder
                        if ligne_count >= 5:
                            break
            else:
                # Si pas de section Autorisations, on prend simplement les premières lignes après Qualités
                rows = response.xpath('//tr[td/h2[contains(text(), "Qualités")]]/following-sibling::tr[position() < 5]')
                for row in rows:
                    # Vérifier si c'est une nouvelle section
                    if row.xpath('./td/h2').get():
                        break
                    
                    qualite = {}
                    
                    # Extraire la description
                    description = row.xpath('./td/text()').get()
                    if description:
                        description = description.strip()
                        if description and "Pas de données reprises dans la BCE." not in description:
                            qualite['description'] = description
                    
                    # Extraire la date
                    date = row.xpath('.//span[@class="upd"]/text()').get()
                    if date and 'Depuis le' in date:
                        qualite['date_debut'] = date.replace('Depuis le', '').strip()
                    
                    if qualite and 'description' in qualite:
                        qualites.append(qualite)
        
        return qualites
    
    def extraire_autorisations(self, response):
        autorisations = []
        
        # Extraction des autorisations
        section = response.xpath('//tr[td/h2[contains(text(), "Autorisations")]]')
        if section:
            # Vérifier s'il y a un message indiquant qu'il n'y a pas de données
            no_data_row = section.xpath('./following-sibling::tr[1]/td[contains(text(), "Pas de données reprises dans la BCE.")]')
            if no_data_row:
                autorisations.append({
                    "description": "Pas de données reprises dans la BCE."
                })
            else:
                # Prendre les lignes qui suivent directement la section Autorisations
                rows = response.xpath('//tr[td/h2[contains(text(), "Autorisations")]]/following-sibling::tr')
                ligne_count = 0
                
                for row in rows:
                    # Arrêter si on atteint une autre section (h2)
                    if row.xpath('./td/h2').get():
                        break
                    
                    autorisation = {}
                    
                    # Tenter d'extraire le texte du lien
                    lien = row.xpath('.//a/text()').get()
                    if lien:
                        autorisation['description'] = lien.strip()
                    
                    # Tenter d'extraire l'URL
                    href = row.xpath('.//a/@href').get()
                    if href:
                        autorisation['url'] = href
                    
                    # Si on n'a pas trouvé de lien, regarder s'il y a du texte directement
                    if not 'description' in autorisation:
                        text = row.xpath('./td/text()').get()
                        if text and text.strip():
                            text = text.strip()
                            if "Pas de données reprises dans la BCE." in text:
                                autorisation['description'] = "Pas de données reprises dans la BCE."
                    
                    if autorisation and 'description' in autorisation:
                        autorisations.append(autorisation)
                    
                    ligne_count += 1
                    # Limiter à 3 lignes pour éviter de déborder
                    if ligne_count >= 3:
                        break
        
        return autorisations
    
    def extraire_nace_codes(self, response, version):
        codes = []
        
        if version == '2025':
            # Extraction des codes NACE 2025
            rows = response.xpath('//tr[td[contains(text(), "TVA 2025")]]')
            for row in rows:
                code = {}
                
                code_nace = row.xpath('./td//a/text()').get()
                if code_nace:
                    code['code'] = code_nace.strip()
                
                description = row.xpath('./td/text()[last()-1]').get()
                if description:
                    code['description'] = description.replace('-', '').strip()
                
                date = row.xpath('./td//span/text()').get()
                if date and 'Depuis le' in date:
                    code['date_debut'] = date.replace('Depuis le', '').strip()
                
                if code:
                    codes.append(code)
        elif version == '2008':
            # Extraction des codes NACE 2008
            rows = response.xpath('//table[@id="toonbtw2008"]//tr[td[contains(text(), "TVA 2008")]]')
            for row in rows:
                code = {}
                
                code_text = row.xpath('./td/text()').getall()
                if code_text and len(code_text) > 1:
                    code_match = re.search(r'(\d+\.\d+)', code_text[0])
                    if code_match:
                        code['code'] = code_match.group(1)
                    
                    description = code_text[1] if len(code_text) > 1 else None
                    if description:
                        code['description'] = description.strip()
                
                date = row.xpath('./td//span/text()').get()
                if date and 'Depuis le' in date:
                    code['date_debut'] = date.replace('Depuis le', '').strip()
                
                if code:
                    codes.append(code)
        elif version == '2003':
            # Extraction des codes NACE 2003
            rows = response.xpath('//table[@id="toonbtw"]//tr[td[contains(text(), "TVA2003")]]')
            for row in rows:
                code = {}
                
                text = row.xpath('./td/text()').get()
                if text:
                    parts = text.split('-')
                    if len(parts) > 1:
                        code_match = re.search(r'(\d+\.\d+)', parts[0])
                        if code_match:
                            code['code'] = code_match.group(1)
                        code['description'] = parts[1].strip()
                
                date = row.xpath('./td//span/text()').get()
                if date and 'Depuis le' in date:
                    code['date_debut'] = date.replace('Depuis le', '').strip()
                
                if code:
                    codes.append(code)
        
        return codes
    
    def extraire_donnees_financieres(self, response):
        financieres = {}
        
        # Extraction du capital
        capital = response.xpath('//tr[td[contains(text(), "Capital")]]/td[2]/text()').get()
        if capital:
            financieres['capital'] = capital.strip()
        
        # Extraction de l'assemblée générale
        ag = response.xpath('//tr[td[contains(text(), "Assemblée générale")]]/td[2]/text()').get()
        if ag:
            financieres['assemblee_generale'] = ag.strip()
        
        # Extraction de la date de fin d'année comptable
        fin_annee = response.xpath('//tr[td[contains(text(), "Date de fin de l\'année comptable")]]/td[2]/text()').get()
        if fin_annee:
            financieres['fin_annee_comptable'] = fin_annee.strip()
        
        return financieres
    
    def extraire_liens_entites(self, response):
        liens = []
        
        # Extraction des liens entre entités
        section = response.xpath('//tr[td/h2[contains(text(), "Liens entre entités")]]')
        if section:
            # Vérifier s'il y a un message indiquant qu'il n'y a pas de données
            no_data_row = section.xpath('./following-sibling::tr[1]/td[contains(text(), "Pas de données reprises dans la BCE.")]')
            if no_data_row:
                liens.append({
                    "description": "Pas de données reprises dans la BCE."
                })
            else:
                rows = section.xpath('./following-sibling::tr[position() < 20 and td/a]')
                for row in rows:
                    lien = {}
                    
                    numero = row.xpath('./td/a/text()').get()
                    if numero:
                        lien['numero'] = numero.strip()
                    
                    nom = row.xpath('./td/text()[1]').get()
                    if nom:
                        lien['nom'] = nom.strip()
                    
                    relation = row.xpath('./td/text()[2]').get()
                    if relation:
                        lien['relation'] = relation.strip()
                    
                    date = row.xpath('./td/text()[3]').get()
                    if date:
                        date_match = re.search(r'depuis le (\d+ \w+ \d+)', date)
                        if date_match:
                            lien['date'] = date_match.group(1)
                    
                    if lien:
                        liens.append(lien)
        
        return liens
    
    def extraire_liens_externes(self, response):
        liens = []
        
        # Extraction des liens externes
        section = response.xpath('//tr[td/h2[contains(text(), "Liens externes")]]')
        if section:
            liens_elements = section.xpath('./following-sibling::tr[1]//a')
            for lien_element in liens_elements:
                lien = {}
                
                texte = lien_element.xpath('./text()').get()
                if texte:
                    lien['description'] = texte.strip()
                
                url = lien_element.xpath('./@href').get()
                if url:
                    lien['url'] = url
                
                if lien:
                    liens.append(lien)
        
        return liens