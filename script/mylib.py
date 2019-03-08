#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
import os	
from glob import glob
import sys
import json
from pprint import pprint
import distance
import requests
import time

import xml.etree.ElementTree as ET
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import HTMLConverter,TextConverter,XMLConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
import io

import re

path = 'data/'
authorsJson = 'output/authorList.01.02.03.json'
outAuthors = 'output/JSON-authors'
outXML = 'output/XML'
outTXT = 'output/TXT'
outTXT_NoWS = 'output/TXT-NoWS'
outTXT_NoWS_NoPageNum = 'output/TXT-NoWS-NoPageNum'
outputTsv = 'output/DBLP-names-manualSubstitution.tsv'

inputTsv = 'output/DBLP-names-manualSubstitution.edited.tsv'
# TO DELETE
authorsJsonOut = 'output/authorList.04.05.06.07.json'
outBIB = "output/BIB"
outputTSV_DBLP = 'output/doiCandidati.DBLP.tsv'
outputTSV_CV = 'output/doiCandidati.CV.tsv'
# TO DELETE
authorsJsonOutDBLP = 'output/authorList.04.05.06.07.DBLP.json'
authorsJsonOutDBLPandCV = 'output/authorList.04.05.06.07.DBLP.CV.json'
authorsJsonOutDBLPandCV_mergedChecked = 'output/authorList.04.05.06.07.DBLP.CV.checkedMerged.json'
multipleDoisOut = 'output/multipleDoisDBLP.json'

rfs = ['01-B1']
fasce = [1, 2]
sessioni = [1, 2, 3, 4, 5]
sleepTimeDBLP = 0.5
sleepTimeDOIdotORG = 0.5

def loadJson(filename):
	with open(filename) as f:
		data = json.load(f)
		return data

# TODO - invertire argomenti (filename <-> data)
def saveJson(filename, data):
	directory = os.path.dirname(filename)
	if not(os.path.isdir(directory) and os.path.exists(directory)):
		os.makedirs(directory)
	
	with open(filename, 'w') as outfile:
		json.dump(data, outfile, ensure_ascii=True, indent=2, sort_keys=True) # 


def searchAuthor(firstname, surname, exactWord=True):
	try:
		if exactWord:
			query = 'https://dblp.org/search/author/api/?q=' + surname + '$ ' + firstname + '$&format=json&h=1000'
		else:
			query = 'https://dblp.org/search/author/api/?q=' + surname + ' ' + firstname + '&format=json&h=1000'
		time.sleep(sleepTimeDBLP)
		r = requests.get(query)
		data = r.json()
		return data
	except requests.exceptions.RequestException as e:
		return {}


# conversione pdf -> txt (carica file se esiste, lo converte e salva altrimenti)
def getTxt(basepath, rf, fascia, sessione, filename, savefile=True):
	if not(os.path.isdir(outTXT) and os.path.exists(outTXT)):
		os.makedirs(outTXT)
	
	txtFilePath = outTXT + '/' + filename.replace(".pdf", ".txt")
	if os.path.isfile(txtFilePath) and os.path.getsize(txtFilePath)>0:
		with open(txtFilePath, 'r') as myfile:
			cvText = myfile.read()
	else:
		filepath = basepath + '/' + rf + '/fascia' + str(fascia) + '/sessione' + str(sessione) + '/'
		cvText = convert('text', filepath + filename) #, pages=[0,1])
		if savefile:
			open(txtFilePath, 'w').write(cvText)
	return cvText
	
	
# conversione pdf -> txt SENZA WHITESPACES (carica file se esiste, lo converte e salva altrimenti)
def getTxtNoWS(basepath, rf, fascia, sessione, filename, savefile=True):
	if not(os.path.isdir(outTXT_NoWS) and os.path.exists(outTXT_NoWS)):
		os.makedirs(outTXT_NoWS)
	
	txtNoWsFilePath = outTXT_NoWS + '/' + filename.replace(".pdf", ".txt")
	if os.path.isfile(txtNoWsFilePath) and os.path.getsize(txtNoWsFilePath)>0:
		with open(txtNoWsFilePath, 'r') as myfile:
			cvText_noWhitespaces = myfile.read()
	else:
		cvText = getTxt(basepath, rf, fascia, sessione, filename, savefile)
		cvText_noWhitespaces = ' '.join(cvText.split())
		if savefile:
			open(txtNoWsFilePath, 'w').write(cvText_noWhitespaces)
	return cvText_noWhitespaces


def getTxtNoWS_NEW(basepath, rf, fascia, sessione, filename, savefile=True):
#def getTxtNoWS_NEW(outputPath, filename, savefile=True):
	if not(os.path.isdir(outTXT_NoWS_NoPageNum) and os.path.exists(outTXT_NoWS_NoPageNum)):
		os.makedirs(outTXT_NoWS_NoPageNum)
	
	txtNoWsFilePath = outTXT_NoWS_NoPageNum + '/' + filename.replace(".pdf", ".txt")
	if os.path.isfile(txtNoWsFilePath) and os.path.getsize(txtNoWsFilePath)>0:
		with open(txtNoWsFilePath, 'r') as myfile:
			cvText_noWhitespaces = myfile.read()
	else:
		print ('%s, %s, %s, %s, %s' % (basepath, rf, fascia, sessione, filename))
		cvText = getTxt(basepath, rf, fascia, sessione, filename, savefile)
		year = '^[\s]*[1-2][0-9]{3}$'
		numpubs = '^[\s]*[1-9][0-9]*$'
		page = '^[\s]*- [1-9][0-9]* -$'
		temp = ''
		for line in cvText.split('\n'):
			linesub = re.sub(year + '|' + numpubs + '|' + page, ' ', line)
			temp += linesub + ' '
		cvText_noWhitespaces = ' '.join(temp.split())
		if savefile:
			open(txtNoWsFilePath, 'w').write(cvText_noWhitespaces)
	return cvText_noWhitespaces

	
def getXML(urlAuthor, outputPath, filename, save=True, load=True):
	if not(os.path.isdir(outputPath) and os.path.exists(outputPath)):
		os.makedirs(outputPath)
	
	filepath = outputPath + "/" + filename.replace(".pdf", ".xml")
	if load and os.path.isfile(filepath) and os.path.getsize(filepath)>0:
		print ('\nOpening file ' + filename.replace('.pdf', '.xml') + '\n')
		with open(filepath, 'r') as myfile:
			xmlText = myfile.read()
	else:
		print ('\nDownloading file ' + filename.replace('.pdf', '.xml') + '\n')
		time.sleep(sleepTimeDBLP)
		r = requests.get(urlAuthor + ".xml")
		if save:
			open(filepath, 'wb').write(r.content)
		xmlText = r.content	#.decode("utf-8") 
	return xmlText


def getBIB(urlAuthor, outputPath, filename, save=True, load=True):
	if not(os.path.isdir(outputPath) and os.path.exists(outputPath)):
		os.makedirs(outputPath)
	
	filepath = outputPath + "/" + filename.replace('.pdf', '.bib')
	if load and os.path.isfile(filepath) and os.path.getsize(filepath)>0:
		print ('\nOpening file ' + filename.replace('pdf', 'bib') + '\n')
		with open(filepath, 'r') as myfile:
			bibText = myfile.read()
	else:
		print ('\nDownloading file ' + filename.replace('pdf', 'bib') + '\n')
		time.sleep(sleepTimeDBLP)
		r = requests.get(urlAuthor + ".xml")
		if save:
			open(filepath, 'wb').write(r.content)
		bibText = r.content	#.decode("utf-8") 
	return bibText


def computePermutations(names, surnames, filename):
	res = []
	for name in names.split():
		for surname in surnames.split():
			res.append([name, surname])
	return res

def convert(case,fname, pages=None):
	if not pages: pagenums = set();
	else:		 pagenums = set(pages);	  
	manager = PDFResourceManager() 
	codec = 'utf-8'
	caching = True

	if case == 'text' :
		output = io.StringIO()
		converter = TextConverter(manager, output, codec=codec, laparams=LAParams())	 
	if case == 'HTML' :
		output = io.BytesIO()
		converter = HTMLConverter(manager, output, codec=codec, laparams=LAParams())

	interpreter = PDFPageInterpreter(manager, converter)   
	infile = open(fname, 'rb')

	for page in PDFPage.get_pages(infile, pagenums,caching=caching, check_extractable=True):
		interpreter.process_page(page)

	convertedPDF = output.getvalue()  

	infile.close(); converter.close(); output.close()
	return convertedPDF


def checkPubs(cvTxtNoWsFullPath, outpubsXML, data, author):
	if not(os.path.isdir(outpubsXML) and os.path.exists(outpubsXML)):
		os.makedirs(outpubsXML)
	with open(cvTxtNoWsFullPath, 'r') as myfile:
		cvText_noWhitespaces = myfile.read()
	filename = author['filename']
	
	# 2.
	hits = numHits = data['result']['hits']['hit']
	maxPubFound = 0
	maxUrl = ''
	maxHit = {}
	maxXmlText = ''
	
	for hit in hits:
		try:
			url = hit['info']['url']
			# 2a.
			#time.sleep(sleepTimeDBLP)
			#r = requests.get(url + ".xml")
			#xmlText = r.content	#.decode("utf-8") 
			xmlText = getXML(url, outpubsXML, filename, False, False)
			
			# 2b.
			root = ET.fromstring(xmlText)
			titles = root.findall('./r/*/title')
			
			# 2c.
			titleList = []
			currPubFound = 0
			for title in titles:
				titleStr = "".join(title.itertext())

				if titleStr.lower() in cvText_noWhitespaces.lower():
					currPubFound += 1
				else:
					pass
			# TODO - TENIAMO SOLO LA HIT CON MAGGIOR NUMERO DI PUBBLICAZIONI -> POTREMMO TENERLE TUTTE
			if currPubFound > maxPubFound:
				maxUrl = url
				maxPubFound = currPubFound
				maxHit = hit
				maxXmlText = xmlText
		except requests.exceptions.RequestException as e:
			print ('ERROR: http request problem.')
			return False
		except ET.ParseError as e:
			print ('ERROR: xml parsing problem.')
			return False
			
	if maxPubFound > 0:
		print ('FOUND - %s has %d publications' % (maxUrl, maxPubFound))
		
		xmlFilePath = outpubsXML + "/" + filename.replace(".pdf", ".xml")
		if not (os.path.isfile(xmlFilePath) and os.path.getsize(xmlFilePath)>0):
			open(xmlFilePath, 'wb').write(maxXmlText)
		
		# 2cii.
		author['hits-dblp'] = [maxHit]
		author['numHits-dblp'] = 1
		return True
	else:
		# NOT FOUND!
		print ('*** NOT FOUND ***')
		return False

def exportTSV(authorsJson, rf, fieldName, outFile):
	res = 'id-dblp\torcid\tfascia\tsessione\tcv filename\tsurname\tname\tdois\n'
	
	for fascia in authorsJson['found'][rf]:
		for sessione in authorsJson['found'][rf][fascia]:
			for author in authorsJson['found'][rf][fascia][sessione]:
				firstname = author['firstname']
				surname = author['surname']
				numHits = author['numHits-dblp']
				filename = author['filename']
				print (filename)
				orcid = ''
				if 'orcid' in author:
					orcid = author['orcid']
				
				if numHits == 1:
					print ('=1: ' + filename)
					dois = author[fieldName]
					idDblp = author['hits-dblp'][0]['info']['url']
					res += '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % (idDblp, orcid, fascia, sessione, filename, surname, firstname, dois)
				else:
					print ('else: ' + filename)
					dois = author[fieldName]
					print ('exportTSV() - file %s: numHits != 1.' % filename)
					res += '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % ('', orcid, fascia, sessione, filename, surname, firstname, dois)
	with open(outFile, "w") as text_file:
		text_file.write(res)

def exportTSV_withDoisInCv(authorsJson, outFile):
	res = 'id-dblp\torcid\tfascia\tsessione\tcv filename\tsurname\tname\tdois DBLP\tdois in CV(exist)\tdois in CV(not exist)\tdois ALL (DBLP+CV)\n'
	for fascia in authorsJson:
		for sessione in authorsJson[fascia]:
			for author in authorsJson[fascia][sessione]:
				firstname = author['firstname']
				surname = author['surname']
				numHits = author['numHits-dblp']
				filename = author['filename']
				orcid = ''
				if 'orcid' in author:
					orcid = author['orcid']
				
				if numHits == 1:
					dois = author['dois']
					doisDBLP_exist = author['doisCVnotDBLP-exist']
					doisDBLP_notExist = author['doisCVnotDBLP-notExist']
					idDblp = author['hits-dblp'][0]['info']['url']
					res += '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % (idDblp, orcid, fascia, sessione, filename, surname, firstname, dois, doisDBLP_exist, doisDBLP_notExist, dois + ', ' + doisDBLP_exist)
				else:
					print ('exportTSV() - file %s: numHits != 1.' % filename)
					doisDBLP_exist = author['doisCVnotDBLP-exist']
					doisDBLP_notExist = author['doisCVnotDBLP-notExist']
					res += '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % ('', orcid, fascia, sessione, filename, surname, firstname, '', doisDBLP_exist, doisDBLP_notExist, ', ' + doisDBLP_exist)
	with open(outFile, "w") as text_file:
		text_file.write(res)


# see http://www.doi.org/factsheets/DOIProxy.html#rest-api for documentation
def checkDoiExist(doi, sleep=0.1): #, repeat=1, sleep=0):
	urlDblpCheckDoi = 'https://doi.org/api/handles/'
	try:
		time.sleep(sleepTimeDOIdotORG)
		r = requests.get(urlDblpCheckDoi + doi)
		data = r.json()
		responseCode = data['responseCode']
		if responseCode == 1: # success
			return True
		elif responseCode == 2: # error -> retry?
			print ('Response cose 2 (error) in checkDoiExist().')
			return False
		else:
			return False
	except requests.exceptions.RequestException as e:
		return {}

