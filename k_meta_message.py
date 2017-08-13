import re
from pprint import pprint

class meta_message():
	#delimeters
	deli = [b'\xef',b'\xbf',b'\xbd',
		b'\x01', b'\x12', b'\x06', 
		b'\x02', b'\x10', b'\x03', 
		b'\x15', b'\x16',b'\x11', 
		b'\x1ef', b'\x1c', b'\x14'
		b'\x04', b'\x1e',b'\x09'
		b'\x19', b'\xf7', b'\x1a'
		b'\c1b', b'\x13', b'\x00',
		b'\x05', b'\x07', b'\x08']
	deli_replace = ';'
	deli_replaceu = r'u0'
	q_msg_meta_ip_re = r'(/([0-9]{3}|[0-9]{2}|[0-9])\.([0-9]{3}|[0-9]{2}|[0-9])\.([0-9]{3}|[0-9]{2}|[0-9])\.([0-9]{3}|[0-9]{2}|[0-9]))'
	q_msg_meta_consumer_re = r'(([_!@#$%~^&*]*[a-zA-Z0-9]*)+){1}'
	q_msg_meta_ct_sub_re = r';;' + q_msg_meta_consumer_re 
	def parse(self, byte_stream):
		if byte_stream == None or len(byte_stream) == 0:
			return ''
		lines = []
		for line in byte_stream:
			try:
				#replace delimiters with ;
				self.deli.index(line)
				lines.append(self.deli_replace)
			except ValueError:
				lines.append(line.decode('ascii', 'replace'))
		str_nbf = ''.join(lines)
		#remove new lines
		no_nl = str_nbf.replace('\n', self.deli_replace)
		#remove white spaces
		no_spc = re.sub(r'\s+', self.deli_replace, no_nl)
		#find ip address for topic
		matches = re.finditer(self.q_msg_meta_ip_re, no_spc)
		match_ip_str = []
		sub_end_index = -1
		for it in enumerate(matches):
			for match in it:
				try:
					sub_index = match.start(0)
					sub_end = match.group(0)
					sub_end_index = sub_index + len(sub_end)
					match_ip_str.append(sub_end)
				except AttributeError:
					match_ip_str.append('')
					sub_end_index = -1
					sub_index = -1
		#remove ip address from string
		if sub_end_index != -1:
			no_spc = no_spc[sub_end_index:]
		#remove u0 from string
		no_del = re.sub(self.deli_replaceu, '', no_spc)
		#find topics string
		matches = re.finditer(self.q_msg_meta_ct_sub_re, no_del)
		match_ct_str = []
		for it in enumerate(matches):
			for match in it:
				try:
					match_ct_str.append(match.group(0))
				except AttributeError:
					match_ct_str.append('')

		#find topics
		matches = re.finditer(self.q_msg_meta_consumer_re, ''.join(match_ct_str))
		match_consumer_str = []
		for it in enumerate(matches):
			for match in it:
				try:
					match_consumer_str.append(match.group(0))
				except AttributeError:
					match_consumer_str.append('')

		#remove duplicate entries
		topics_no_dup = []
		for topic in match_consumer_str:
			try:
				topics_no_dup.index(topic)
			except ValueError:
				topics_no_dup.append(topic)


		return (match_ip_str, topics_no_dup)
