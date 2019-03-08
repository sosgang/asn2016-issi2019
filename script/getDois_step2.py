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
import csv

import xml.etree.ElementTree as ET
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import HTMLConverter,TextConverter,XMLConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
import io

import re

import mylib



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
inputTsv = mylib.inputTsv
outpubsBIB = mylib.outBIB
authorsJsonOutDBLPandCV = mylib.authorsJsonOutDBLPandCV
multipleDoisOut = mylib.multipleDoisOut
authorsJsonOutDBLPandCV_mergedChecked = mylib.authorsJsonOutDBLPandCV_mergedChecked

# Recupera DOI da DBLP (usando XML scaricato) e setta 'dois-DBLP' nel JSON
def computeDOIsDBLP(authorsJson):
	multipleDois = []
	for rf in authorsJson['found']:
		for fascia in authorsJson['found'][rf]:
			for sessione in authorsJson['found'][rf][fascia]:
				for author in authorsJson['found'][rf][fascia][sessione]:
					filename = author['filename']
					print (filename)
					xmlpath = (outXML + '/' + filename.replace('.pdf', '.xml'))
					dois = []
					if os.path.exists(xmlpath):
						try:
							tree = ET.parse(xmlpath)
							root = tree.getroot()
							papers = root.findall('./r')
							doiPapersList = []
							for paper in papers:
								ees = paper.findall('./*/ee')
								if (len(ees)) != 0:
									doiPaperList = []
									foundDoi = False
									for ee in ees:
										eeTesto = "".join(ee.itertext())
										# FPOGGI - SILVIO'S REGEX
										#matchDoi = re.search('10\.[^/]+/.+', eeTesto)
										# FPOGGI - PG'S REGEX
										#matchDoi = re.search('10\.[^/]{4}/[^\s]+', eeTesto)
										matchDoi = re.search('10\.[^/]+/[^\s]+', eeTesto)
										if matchDoi:
											doi = eeTesto[matchDoi.span()[0]:]
											if doi not in doiPaperList:
												doiPaperList.append(doi)
											foundDoi = True
									if foundDoi:
										# (Non dovrebbe succedere) Aggiungo DOI se non già presente
										if doiPaperList[0] not in doiPapersList:
											doiPapersList.append(doiPaperList[0])
										
									if len(doiPaperList) > 1:
										multipleDois.append(doiPaperList)
								else:
									pass
						except ET.ParseError as e:
							print ('ERROR: xml parsing problem in computeDOIs().')
							sys.exit()
				
						author['dois-DBLP'] = doiPapersList
						print ('added dois')
					else:
						print ('ERROR in computeDOIsDBLP(): xmlpath doesn\'t exist.')
	
	if len(multipleDois) > 0:
		print ('MULTIPLE DOIS: FOUND')
		mylib.saveJson(multipleDoisOut, multipleDois)
	else:
		print ('MULTIPLE DOIS: NOT FOUND')
	
	return authorsJson


# Recupera DOI da CV (usando regex) e setta 'dois-CV' nel JSON
def computeDOIsCV(authorsJson):
	i = 1
	for rf in authorsJson['found']:
		for fascia in authorsJson['found'][rf]:
			for sessione in authorsJson['found'][rf][fascia]:
				for author in authorsJson['found'][rf][fascia][sessione]:
					filename = author['filename']
					print (str(i) + ') ' + filename)
					if 'dois-DBLP' in author:
						doisXmlDBLP = author['dois-DBLP']
					else:
						doisXmlDBLP = []
					
					# txt without whitespaces - loads file if exists 
					cvText = mylib.getTxt(path, rf, fascia, sessione, filename)
					#cvText_noWhitespaces = mylib.getTxtNoWS(path, rf, fascia, sessione, filename)
					cvText_noWhitespaces = mylib.getTxtNoWS_NEW(path, rf, fascia, sessione, filename)
					
					#matchDoi = re.search('10\.[^/]+/.+', cvText_noWhitespaces)
					#matchDoi = re.findall('10\.[^/]+/.+ ', cvText_noWhitespaces)
					matchDois = re.findall('10\.[^/]{4}/[^\s]+', cvText_noWhitespaces)
					#matchDois = re.findall('10\.[^/^\s]+/[^\s]+', cvText_noWhitespaces)
					
					doisCV = []
					newDois_exist = []
					newDois_notExist = []
					for matchDoi in matchDois:
						# Tolgo caratteri terminali (es. punteggiature) 
						finalChars = ('"', '#', ')', ',', '-', '.', '/', ';', ']', '}')		
						while matchDoi.endswith(finalChars):
							matchDoi = matchDoi[:len(matchDoi)-1]
						#Aggiungo DOI se non già presente
						if matchDoi not in doisCV:
							doisCV.append(matchDoi)
						
						
						#############################################
						#if mylib.checkDoiExist(matchDoi):
							#newDois_exist.append(matchDoi)
						#else:
							#print ('Not exists: ' + matchDoi)
							#lines = cvText.split('\n')
							#print ('lines: %d' % len(lines))
							
							
							#for il in range(0, len(lines)):
								#currline = lines[il]
								#if currline.find(matchDoi) >= 0:
									#inl = 1
									#while lines[il+inl] == '':
										#inl += 1
									
									#nextline = lines[il+inl]
									#print (filename)
									#print (currline)
									#print (lines[il+1])
									#print ('-' + nextline + '-')
									##continue
									#newdoi = matchDoi + nextline.split(' ')[0]
									#print (newdoi)
									
									#while newdoi.endswith(finalChars):
										#newdoi = newdoi[:len(newdoi)-1]
									
									#if mylib.checkDoiExist(newdoi):
										#print ('Trovato: %s -> %s' % (matchDoi, newdoi))
										#newDois_exist.append(newdoi)
									#else:
										#print ('\tNON Trovato: %s -> %s' % (matchDoi, newdoi))
										#newDois_notExist.append(matchDoi)
						##sys.exit()
						#############################################
					i += 1
					#author['doisCVnotDBLP-exist'] = ', '.join(newDois_exist)
					#author['doisCVnotDBLP-notExist'] = ', '.join(newDois_notExist)
					author['dois-CV'] = doisCV
					
		return authorsJson


# Se non presente, scarica XML e BIB usando URL dell'autore nel JSON
def getPubsXMLandBIB(authorsJson):
	i = 1
	for rf in authorsJson['found']:
		for fascia in authorsJson['found'][rf]:
			for sessione in authorsJson['found'][rf][fascia]:
				for author in authorsJson['found'][rf][fascia][sessione]:
					filename = author['filename']
					print ('%d %s' % (i, filename))
					numHitsDblp = author['numHits-dblp']
					if numHitsDblp == 0:
						print ('ERROR *** %s has no hits-dblp' % author['filename'])
						sys.exit()
					elif numHitsDblp == 1:
						try:
							url = (author['hits-dblp'][0]['info']['url'])
							mylib.getXML(url, outXML, filename, True, False)
							mylib.getBIB(url, outBIB, filename, True, False)
						except requests.exceptions.RequestException as e:
							print ('ERROR: http request problem.')
							sys.exit()
					else:
						print ('*** %s has too many hits-dblp' % author['filename'])
						sys.exit()
					i += 1


# Usa TSV con nomi editato manualmente per cercare candidati mancanti in DBLP e completare il JSON
def searchUsingTsv(tsvFile, jsonData):
	with open(tsvFile, 'r') as tsv:
		tsv.readline()
		myreader = csv.reader(tsv, delimiter='\t', quotechar='"')
		for row in myreader:
			filename = row[0]
			nameDblp = row[1]
			print (filename)
			for rf in jsonData['not-found']:
				for fascia in jsonData['not-found'][rf]:
					for sessione in jsonData['not-found'][rf][fascia]:
						for author in jsonData['not-found'][rf][fascia][sessione]:
							if author['filename'] == filename:
								if nameDblp == '':
									nameDblp = author['surname']
								
								data = mylib.searchAuthor('', nameDblp, False)
								if not data:
									# an error in searchAuthor occurred
									print ('NO DATA: ' + filename_withPath)
									author['hits-dblp'] = []
									author['numHits-dblp'] = 0
									print (author)
									
								numHits = data['result']['hits']['@computed']
								numHits2 = data['result']['hits']['@total']
								if numHits != numHits2:
									print ("*** PROBLEMA ***")
									sys.exit()
								
								if int(numHits) == 0:
									print ('numHits = 0')
									author['hits-dblp'] = []
									author['numHits-dblp'] = 0
								elif int(numHits) >= 1:
									print ('numHits >= 1')
									#txtNoWsFilePath = outTXT_NoWS + '/' + filename.replace(".pdf", ".txt")
									txtNoWsFilePath = outTXT_NoWS_NoPageNum + '/' + filename.replace(".pdf", ".txt")
									
									# TODO - CONTROLLO PUBS in data
									if mylib.checkPubs(txtNoWsFilePath, outXML, data, author):
										jsonData['not-found'][rf][fascia][sessione].remove(author)
										if int(numHits) > 1:
											print ('2+ hits: ' + txtNoWsFilePath)
										author['jsonfiles-author'] = filename.replace('.pdf', '.json')
										mylib.saveJson(outAuthors + "/" + filename.replace('.pdf', '.json'), data)
										jsonData['found'][rf][fascia][sessione].append(author)
									else:
										author['hits-dblp'] = []
										author['numHits-dblp'] = 0
								else:
									print ("ERROR - NumHits: " + numHits)
									sys.exit()
									
				print (filename + '- END')
		
		return jsonData


def checkDoisDBLPandCV(authorsJson):
	i = 1
	for rf in authorsJson['found']:
		for fascia in authorsJson['found'][rf]:
			for sessione in authorsJson['found'][rf][fascia]:
				for author in authorsJson['found'][rf][fascia][sessione]:
					filename = author['filename']
					#if filename != '24377_BUSCEMA_Paolo_Massimo.pdf':
					#if filename != '65094_SANTORO_Carmelina.pdf':
					#	continue
					
					print (filename)
					if 'dois-CV' not in author:
						print ('N0 dois-CV: ' + filename)
						doisCV = []
					else:
						doisCV = author['dois-CV']
					doisCV_exist = []
					doisCV_notExist = []
					for doiCV in doisCV:
						if mylib.checkDoiExist(doiCV):
							doisCV_exist.append(doiCV)
						else:
							doisCV_notExist.append(doiCV)
					author['dois-CV-exist'] = doisCV_exist
					
					if 'dois-DBLP' not in author:
						print ('No dois-DBLP: ' + filename)
						doisDBLP = []
					else:
						doisDBLP = author['dois-DBLP']
					doisDBLP_exist = []
					doisDBLP_notExist = []
					for doiDBLP in doisDBLP:
						if doiDBLP not in doisCV_exist:
							if mylib.checkDoiExist(doiDBLP):
								doisDBLP_exist.append(doiDBLP)
							else:
								doisDBLP_notExist.append(doiDBLP)
					author['dois-DBLP-exist'] = doisDBLP_exist

					doisCV_DBLP_exist = []
					for doi in doisCV_exist:
						doisCV_DBLP_exist.append(doi)
					for doi in doisDBLP_exist:
						if doi not in doisCV_DBLP_exist:
							doisCV_DBLP_exist.append(doi)
					author['dois-CV-DBLP-exist'] = doisCV_DBLP_exist
					'''
					print ('DBLP not in CV')
					for doi in author['dois-DBLP']:
						if doi not in author['dois-CV']:
							print (doi + ' - ' + filename)
					print ('CV not in DBLP')
					for doi in author['dois-CV']:
						if doi not in author['dois-DBLP']:
							print (doi + ' - ' + filename)
					'''
	return authorsJson
	
# 04
authors = mylib.loadJson(authorsJson)
authorsV2 = searchUsingTsv(inputTsv, authors)
#mylib.saveJson(authorsJsonOut, authorsV2)
#authorsV2 = mylib.loadJson(authorsJsonOut)

# 06
getPubsXMLandBIB(authorsV2)


# 07DBLP
authorsV2DOIsDBLP = computeDOIsDBLP(authorsV2)
# TO COMMENT
#mylib.saveJson(authorsJsonOutDBLP, authorsV2DOIsDBLP)
#mylib.exportTSV(authorsV2DOIsDBLP, '01-B1', 'dois-DBLP', outputTSV_DBLP)
#
#authorsV2DOIsDBLP = mylib.loadJson(authorsJsonOutDBLP)


# 07CV e 09
authorsV3DoisDBLPandCV = computeDOIsCV(authorsV2DOIsDBLP)
#mylib.saveJson(authorsJsonOutDBLPandCV, authorsV3DoisDBLPandCV)
#########mylib.exportTSV(authorsV2DOIsDBLP, '01-B1', 'dois-CV', outputTSV_CV)
#
#authorsV3DoisDBLPandCV = mylib.loadJson(authorsJsonOutDBLPandCV)
authorsV4DoisDBLPandCV_checkedMerged = checkDoisDBLPandCV(authorsV3DoisDBLPandCV)
mylib.saveJson(authorsJsonOutDBLPandCV_mergedChecked, authorsV4DoisDBLPandCV_checkedMerged)
