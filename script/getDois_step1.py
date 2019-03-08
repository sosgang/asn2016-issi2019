#!/usr/bin/python
# -*- coding: utf-8 -*-

import os	
from glob import glob
import sys
import json
from pprint import pprint
import distance
import requests
import time

import mylib

class AutoVivification(dict):
	"""Implementation of perl's autovivification feature."""
	def __getitem__(self, item):
		try:
			return dict.__getitem__(self, item)
		except KeyError:
			value = self[item] = type(self)()
			return value
            
path = mylib.path
authorsJson = mylib.authorsJson
outAuthors = mylib.outAuthors
outXML = mylib.outXML
outTXT = mylib.outTXT
outTXT_NoWS = mylib.outTXT_NoWS
outTXT_NoWS_NoPageNum = mylib.outTXT_NoWS_NoPageNum
outputTsv = mylib.outputTsv

rfs = mylib.rfs
fasce = mylib.fasce
sessioni = mylib.sessioni
sleepTime = mylib.sleepTimeDBLP

def getAuthorList(basepath):
	# creo output folder
	if not(os.path.isdir(outAuthors) and os.path.exists(outAuthors)):
		os.makedirs(outAuthors)
	
	for rf in rfs:
		resFound = AutoVivification()
		resNotFound = AutoVivification()
		for fascia in fasce:
			for sessione in sessioni:
				resListFound = []
				resListNotFound = []
				resFound[rf][fascia][sessione] = resListFound
				resNotFound[rf][fascia][sessione] = resListNotFound

				filepath = basepath + '/' + rf + '/fascia' + str(fascia) + '/sessione' + str(sessione) + '/'
	
				contents = glob(filepath + '*.pdf')
				contents.sort()
				for filename_withPath in contents:
					print ('*** ' + filename_withPath)
					
					# convert pdf to txt and save to file
					filename = os.path.basename(os.path.normpath(filename_withPath))
					
					cvText = mylib.getTxt(path, rf, fascia, sessione, filename)
					cvText_noWhitespaces = mylib.getTxtNoWS(path, rf, fascia, sessione, filename)
					cvText_new = mylib.getTxtNoWS_NEW(path, rf, fascia, sessione, filename)

					txtNoWsFilePath = outTXT_NoWS + '/' + filename.replace(".pdf", ".txt")
					#txtNoWsFilePath = outTXT_NoWS_NoPageNum + '/' + filename.replace(".pdf", ".txt")
					
					# recupero nome, cognome, idCV e filename
					temp = filename.split('_')
					name = ''
					for i in range(len(temp)):
						if i == 0:
							cv_id = temp[i]
						elif i == (len(temp)-1):
							name += ' ' + temp[i].replace('.pdf','')
						elif i == 1:
							name += temp[i]
						else:
							name += ' ' + temp[i]
					surname = ''
					firstname = ''
					for word in name.split(" "):
						if word.isupper():
							surname += " " + word
						else:
							firstname += " " + word
					
					author = {
						'firstname': firstname[1:], 
						'firstname-dblp': firstname[1:], 
						'surname': surname[1:], 
						'surname-dblp': surname[1:], 
						'filename': filename, 
						'id-cv': cv_id
					}
					print (firstname[1:] + ' - ' + surname[1:])
					if not (os.path.isfile(outAuthors + "/" + filename.replace('.pdf', '.json')) and os.path.getsize(outAuthors + "/" + filename.replace('.pdf', '.json'))>0):
						data = mylib.searchAuthor(firstname[1:], surname[1:], False)
					else:
						data = mylib.loadJson(outAuthors + "/" + filename.replace('.pdf', '.json'))
					if not data:
						# an error in searchAuthor occurred
						print ('NO DATA: ' + filename_withPath)
						author['hits-dblp'] = []
						author['numHits-dblp'] = 0
						resListNotFound.append(author)
						continue
					numHits = data['result']['hits']['@computed']
					numHits2 = data['result']['hits']['@total']
					if numHits != numHits2:
						print ("*** PROBLEMA ***")
						sys.exit()
					
					if int(numHits) == 0:
						print ('numHits = 0')
						# provo con le permutazioni di nome e cognome
						namesSurnames = mylib.computePermutations(firstname, surname, filename)
						
						i=1
						for nameSurname in namesSurnames:
							if not (os.path.isfile(outAuthors + "/" + filename.replace('.pdf', '.json')) and os.path.getsize(outAuthors + "/" + filename.replace('.pdf', '.json'))>0):
								data = mylib.searchAuthor(nameSurname[0], nameSurname[1], False)
							else:
								data = mylib.loadJson(outAuthors + "/" + filename.replace('.pdf', '.json'))
							if not data:
								# an error in searchAuthor occurred
								print ('\tNO DATA: ' + filename_withPath)
								author['hits-dblp'] = []
								author['numHits-dblp'] = 0
								resListNotFound.append(author)
								continue
							numHits = data['result']['hits']['@computed']
							numHits2 = data['result']['hits']['@total']
							if numHits != numHits2:
								print ("*** PROBLEMA ***")
								sys.exit()
							if int(numHits) == 0:
								if i == len(namesSurnames):
									print ('\tnumHits = 0')
									author['hits-dblp'] = []
									author['numHits-dblp'] = 0
									resListNotFound.append(author)
							elif int(numHits) >= 1:
								print ('\tnumHits >= 1')
								# TODO - CONTROLLO PUBS in data
								if mylib.checkPubs(txtNoWsFilePath, outXML, data, author):
									author['jsonfiles-author'] = filename.replace('.pdf', '.json')
									resListFound.append(author)
									mylib.saveJson(outAuthors + "/" + filename.replace('.pdf', '.json'), data)
									break
								else:
									if i == len(namesSurnames):
										author['hits-dblp'] = []
										author['numHits-dblp'] = 0
										resListNotFound.append(author)
							else:
								print ("ERROR - NumHits: " + numHits)
								sys.exit()
							i += 1
					elif int(numHits) >= 1:
						print ('numHits >= 1')
						# TODO - CONTROLLO PUBS in data
						if mylib.checkPubs(txtNoWsFilePath, outXML, data, author):
							if int(numHits) > 1:
								print ('2+ hits: ' + filename_withPath)
							author['jsonfiles-author'] = filename.replace('.pdf', '.json')
							resListFound.append(author)
							mylib.saveJson(outAuthors + "/" + filename.replace('.pdf', '.json'), data)
						else:
							author['hits-dblp'] = []
							author['numHits-dblp'] = 0
							resListNotFound.append(author)
					else:
						print ("ERROR - NumHits: " + numHits)
						sys.exit()

		mylib.saveJson(authorsJson, {
			'found': resFound,
			'not-found': resNotFound
		})
		
		resTsvText = 'filename NOT found\tDBLP name (to fill)\n'
		for rf in resNotFound:
			for fascia in resNotFound[rf]:
				for sessione in resNotFound[rf][fascia]:
					for author in resNotFound[rf][fascia][sessione]:
						print ('fascia%s/sessione%s_01-B1/%s' % (fascia, sessione, author['filename']))
						resTsvText += author['filename'] + '\t\n'

		with open(outputTsv, "w") as text_file:
			text_file.write(resTsvText)		
	
getAuthorList(path)
