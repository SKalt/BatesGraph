# -*- coding: utf-8 -*-
"""
Created on Thu Dec 24 12:27:32 2015

@author: steven
"""

# import block
import os
import re
import string
from lxml import html
from datetime import datetime
from neo4j.v1 import GraphDatabase, basic_auth
import progressbar as pb

class Bates(object):
    "A class to organize the basic scraping functions"
    def __init__(self):
        """
        Populates object attributes
        >>>
        """
#        self.splash_pg = html.parse('http://www.bates.edu/catalog/?s=current')
        self.year_link_map = self.map_years()
        self.dept_page_queries = self.get_dept_extensions()
        self.page_query_tuples = self.generate_page_query_tuples()
        maps = self.map_codes()
        self.maps = maps        
        
    def map_years(self, force_page=False):
        """
        returns a dict mapping urlencoded year queries to academic year strs.
        Args:
            force_page: a boolean, whether or not to force refresh the dept
            list from the bates catalog splash page.
        Returns:
            as above.
        """
        def get_page_xml():
            url = 'http://www.bates.edu/catalog/?a=catalogList'
            page = html.parse(url)
            new_catalog_list = page.xpath('//*[@id="catmenu"]')[0]
            with open('./cached_xml/YearMap.xml', 'w') as xml_file:
                xml = html.etree.tostring(new_catalog_list).decode('utf8')
                xml_file.write(xml)
            return new_catalog_list
        
        if os.path.exists('./cached_xml/YearMap.xml'):
            if force_page:
                catalog_list = get_page_xml()
            else:
                catalog_list = html.parse('./cached_xml/YearMap.xml').getroot()
        else:
            catalog_list = get_page_xml()
        links = catalog_list.xpath('//*[@id="catmenu"]//a')
        string_map = {i.attrib['href']:i.text[:9] for i in links}
        int_map = {}
        for key in string_map:
            int_map[key] = [int(i) for i in re.split(r'\W', string_map[key])]
        if datetime.now().year not in [int_map[k][0] for k in int_map]:
            if not force_page: # in which case the most recent page is cached
                int_map = self.map_years(force_page=True)
        return int_map

    def get_dept_extensions(self, force_page=False):
        """
        Returns a urlencoded queries specifying departments.
        Args:
            force_page: a boolean whether to force refresh the page
        """
        def get_page_xml():
            pdb.set_trace()
            url = 'http://www.bates.edu/catalog/?s=current'
            page = html.parse(url)
            new_dept_list = page.xpath('//*[@id="deptList"]')[0]
            subj_name = page.xpath('//*[@class="subjName"]')[0]
            subj_code = page.xpath('//*[@class="subjCode"]')[0]
            subj_code_2 = page.xpath('//*[@class="subjCodeInt"]')[0]
            with open('./cached_xml/SplashPage.xml', 'w') as xml_file:
                xml = '<body>'
                xml += html.etree.tostring(subj_name).decode('utf8')
                xml += html.etree.tostring(subj_code).decode('utf8')
                xml += html.etree.tostring(subj_code_2).decode('utf8')
                xml += '</body>'
                xml_file.write(xml)
            return page                           
        
        if os.path.exists('./cached_xml/SplashPage.xml'):
            if force_page:
                dept_list = get_page_xml()
            else:
                dept_list = html.parse('./cached_xml/SplashPage.xml')
        else:
            dept_list = get_page_xml()
        dept_list_xpath = '//*[@id="deptList"]/div/li/a/@href'
        results = []
        for href in dept_list.xpath(dept_list_xpath):
            results.append(href.replace('?s=current', ''))
        return results
        
    def generate_page_query_tuples(self):
        """
        Returns a list of tuples of dept_query, year_query's
        """
        results = []
        for year in self.year_link_map:
            if self.year_link_map[year][0] > 2014:
                for dept in self.dept_page_queries:
                    results.append((dept, year))
        return results

    def map_codes(self):
        """
        Returns a dict mapping all department names to department codes and 
        another dict mapping all interdisciplinary department shortcodes to 
        department codes.

        Returns:
            At tuple of three dicts, DEPT:DT and Dept Name:DEPT.
        """
        
        if os.path.exists('./cached_xml/SplashPage.xml'):
            page = html.parse('./cached_xml/SplashPage.xml')
        else:
            raise FileNotFoundError('No cached splash page')

        subj_name = page.xpath(".//div[@class='subjName']//li/text()")
        if not subj_name:
            raise ValueError('No results; check xpath')
        subj_name = [i.replace('and', '&') for i in subj_name]
        subj_code = page.xpath(".//div[@class='subjCode']//li/text()")
        subj_code_2 = page.xpath(".//div[@class='subjCodeInt']//li/text()")
        if not len(subj_code) == len(subj_code_2) == len(subj_name):
            raise ValueError('unequal-length code lists')
        else:
            shortcode_dept_map = {}
            names_dept_map = {}
            dept_name_map = {}
            for i in range(len(subj_name)):
                names_dept_map[subj_name[i]] = subj_code[i]
                shortcode_dept_map[subj_code_2[i]] = subj_code[i]
                dept_name_map[subj_code[i]] = subj_name[i]
            return shortcode_dept_map, names_dept_map, dept_name_map

class Page(object):
    "Holds the functions to scrape a Bates catalog page"

    def __init__(self, dept_query, year_query):
        """
        Requires a Bates instance BATES.
        """
        if not os.path.exists('cached_xml'):
            os.mkdir('cached_xml')
        self.years = BATES.year_link_map[year_query]
        self.url = 'http://www.bates.edu/catalog/' + year_query + dept_query
        self.dept = dept_query.replace('&a=renderDept&d=', '')
        fname = '{}{}.xml'.format(self.dept, self.years[0])
        if os.path.exists('cached_xml/' + fname):
            print('parsing ' + fname)
            self.page = html.parse('cached_xml/' + fname)
        else:
            print('downloading, parsing ' + fname)
            self.page = html.parse(self.url)
            self.cache(fname)
        self.courses = []

    def cache(self, fname):
        """
        Caches the page.
        """
        xml_file = ''
        for div in self.page.xpath('//*[@class="Course"]'):
            # each div describes a course; cache only these.
            xml_file += html.etree.tostring(div).decode('utf8')
        with open('cached_xml/' + fname, 'w') as target:
            target.write(xml_file)

    def parse(self):
        "create Courses out of all course divs within the page"
        for div in self.page.xpath('//*[@class="Course"]'):
            name = div.xpath('./h4[@class="crsname"]/text()')[0]
            # code = name.split('.')[0]
            div_id = div.xpath('./a[2]/@name')[0]
            desc = '\n'.join(div.xpath('./span[@class="CourseDesc"]/text()'))
            concs = div.xpath('./span/div/ul/li/a/text()') # concentrations
            course = Course(self, name, div_id, desc, concs, SESSION)
            self.courses.append(course)

class Course(object):
    "Represents a course description"

    def __repr__(self):
        return '<Course {}, {}>'.format(self.code, self.start_year)
    
    def __init__(self, parent_page, name, div_id, desc, program_tags, session):
        """
        Parses html course info into a this Course object.

        Args:
            parent_page: a Page object on which this course is found.
            name:
            div_id:
            desc:
            concs:
            session:
        """
        self.code, self.title = [i.strip() for i in name.split('.', 1) if i]
        self.link = parent_page.url + '#' + div_id
        self.start_year, self.end_year = parent_page.years
        self.dept = parent_page.dept
        self.desc = desc
        self.program_membership = program_tags
        self.session = session

    def parse_requirements(self):
        """
        Populates self.requirements
        """
        if 'Prerequisite(s):' in self.desc:
            requirements = []
            reqs = self.desc.split('Prerequisite(s):')[1].split('.')[0].strip()
            current_dept = self.dept
            # remove all punctuation from the str describing the prerequisites
            translator = str.maketrans({k:' ' for k in string.punctuation})
            reqs = reqs.translate(translator)
            reqs = (i for i in reqs.split())
            count = 0
            group = {'type':None, 'courses':[]}
            for chunk in reqs:
                if chunk.isupper() and chunk.isalpha() and len(chunk) > 2:
                    # this is a department code
                    current_dept = chunk
                elif len(chunk) >= 3 and chunk[1].isnumeric():
                    # this is a course number
                    group['courses'].append(current_dept + ' ' + chunk)
                    if group['type']:
                        # courses in this group are interchangeable prereqs
                        for course in group['courses']:
                            req = Prerequisite(self, course, group['type'])
                            requirements.append(req)
                        group = {'type':None, 'courses':[]}
                elif chunk.lower() == 'or':
                    group['type'] = chunk + str(count)
                    count += 1
            for course in group['courses']:
                req = Prerequisite(self, course, group['type'])
                requirements.append(req)
            return requirements

    def parse_professors(self):
        """
        Populates self.professors.
        """
        desc = self.desc.split('Prerequisite(s)')[::-1][0]
        profs = re.findall(r' [A-Z]\. [A-Z]\w+[-]?\w+| staff| Staff', desc)
        if not profs:
            raise ValueError('Course description does not have teacher listed')
        else:
            profs = [i.strip() for i in profs]
            results = []
            for i in profs:
                if i.lower() == 'staff':
                    results.append(Taught(self, 'Staff'))
                else:
                    results.append(Taught(i, self.code, self.session))
            return results

    def parse_program_membership(self):
        "parses both interdisciplinary program and concentration membership"
        programs = []
        for program in self.program_membership:
            # program is a string.
            programs.append(Program(program))
        return programs
        
    def merge(self):
        # check if the course is already present
        years = self.session.run(
            """
            MATCH (c:Course) WHERE c.code = {} RETURN c.years
            """.format(self.code)
            )
        years = [record[0]['years'] for record in years]
        if years is None or self.start_year not in years:
            self.session.run(
                """
                MERGE (n:Course {{code:{code}}})
                ON CREATE SET
                    n.link = {link},
                    n.title = {title},
                    n.desc = {desc},
                    n.requirements_flag = {requirements_flag}
                    n.years = [{year}]
                ON MERGE SET
                    n.link = {link},
                    n.title = {title},
                    n.desc = {desc},
                    n.requirements_flag = {requirements_flag}
                    n.years = n.start_year + {start_year}
                """.format(
                    code=self.code,
                    link=self.link,
                    title=self.title,
                    desc=self.desc,
                    requirements_flag=(self.requirements is not None),
                    start_year=self.start_year,
                    end_year=self.end_year
                )
            )


    def merge_profs(self):
        profs = self.parse_professors()
        for prof in profs:
            prof.merge()
            
    def merge_prereqs(self):
        prereqs = self.parse_prerequisites()
        for prereq in prereqs:
            prereq.merge()
            
    def merge_program_membership(self):
        concs = self.parse_concentrations()
        for conc in concs:
            conc.merge()

class Dept(object):
    def __init__(self, course_inst, dept_code):
        self.session = course_inst.session
        self.member = course_inst.code
        self.dept_code = dept_code
        self.year = couse_inst.start_year
        
    def merge(self):
        # ensure the dept exists
        dept_years = self.session.run(
            """
            MERGE (d {{code:{code}}})
            ON CREATE SET
                d.years = [{year}]
            RETURN d.years
            """.format(year=self.year, code=self.dept_code)
            )
        dept_years = [record[0]['years'] for record in dept_years]
        # ensure dept has current course year
        if self.year not in dept_years:
            self.session.run(
                """
                MATCH (d:Dept)
                WHERE d.code = {dept_code}
                SET d.years = d.years + {year}
                RETURN d.years
                """.format(
                    dept_code=self.dept_code,
                    year=self.year
                )
            )
        # check course is linked to department / has course year
        link_years = self.session.run(
            """
            MATCH (c:Course), (d:Dept)
            WHERE c.code = {code} AND d.code = {dept_code}
            MERGE  (c) -[i:In_Dept]-> (d)
            ON CREATE SET
                i.years = [{year}]
            RETURN i.years
            """.format(
                code=self.member,
                dept_code=self.dept_code,
                year=self.year
                )
        )
        link_years = [record[0]['years'] for record in link_years]
        # ensure link in appropriate year
        if self.year not in link_years:
            self.session.run(
                """
                MATCH (c:Course), (d:Dept)
                WHERE c.code = {code} AND d.code = {dept_code}
                MERGE (c) -[i:In_Dept]-> (d)
                ON MATCH SET
                i.years = i.years + {year}
                ON CREATE SET
                i.years = [{year}]
                RETURN i.years
                """.format(
                    code=self.member,
                    dept_code=self.dept_code,
                    year=self.year
                    )
                )
    
class Prerequisite(object):

    def __init__(self, course_inst, required_course_code, label):
        self.requirer = course_inst.code
        self.required = required_course_code
        self.year = course_inst.start_year
        self.label = label
        self.session = course_inst.session

    def merge(self):
        # ensure a link from all 
        link_years = self.session.run(
            """
            MERGE (n:Course {{code:{n}}}) -[r:Prereq_To]-> (m:Course {{code:{m}}})
            ON CREATE SET r.years = [{year}], r.label = {label}
            RETURN r.years
            """.format(
                n=self.required,
                m=self.requirer,
                year=self.year,
                label=self.label
            )
        )
        link_years = [record[0]['years'] for record in link_years][0]
        # update requiremnt year, label
        if self.year not in link_years:
            self.session.run(
                """
                MATCH (n:Course) -[r:Prereq_To] -> (m:Course)
                WHERE m.code = {} AND n.code = {}
                SET r.years = r.years + {}, r.label = {}
                """.format(
                    self.required,
                    self.requirer,
                    self.year,
                    self.label
                )
            )
            
    def __repr__(self):
        return '<Prereq {}->{} {}>'.format(
            self.required,
            self.requirer,
            self.year
        )

class Program(object):
    ""
    def __init__(self, course_inst, concentration_name):
        self.year = course_inst.start_year
        self.course_code = course_inst.code
        self.conc_code = re.search(r'C\d\d\d', concentration_name)
        if self.conc_code:
            self.conc_code = self.conc_code.group()
        self.name = concentration_name.split('(')[0]
        self.session = course_inst.session

    def __repr__(self):
        return '<Program {} {}>'.format(
            self.name,
            self.year
            )
    
    def merge(self):
        ""
        conc_years = self.session.run(
            """
            MERGE (conc:Conentration {{code:{code}}})
            ON CREATE SET
                conc.years = [{year}]
                conc.name = {name}
            RETURN conc.years
            """.format(
                code=self.conc_code,
                year=self.year,
                name=self.name
                )
            )
        conc_years = [record[0]['years'] for record in conc_years]
        if self.year not in conc_years:
            self.session.run(
                """
                MERGE (conc:Conentration {{code:{code}}})
                ON MATCH SET
                    conc.years = conc.years + {year}
                RETURN conc.years
                """.format(
                    code=self.conc_code,
                    year=self.year,
                )
            )
        link_years = self.session.run(
            """
            MATCH (c:Course), (conc:Concentration)
            WHERE c.code = {course_code} AND conc.code = {conc_code}
            MERGE (c) -[i:In_Concentration]-> (conc)
            ON CREATE SET
            i.years = [{year}]
            RETURN i.years
            """.format(
                course_code=self.course_code,
                year=self.year,
                conc_code =self.conc_code
                )
            )
        link_years = [record[0]['years'] for record in link_years]
        if self.year not in link_years:
            self.session.run(
                """
                MATCH (c:Course), (conc:Concentration)
                WHERE c.code = {course_code} AND conc.code = {conc_code}
                MERGE (c) -[i:In_Concentration]-> (conc)
                ON MATCH SET
                i.years = [{year}]
                RETURN i.years
                """.format(
                    course_code=self.course_code,
                    year=self.year,
                    conc_code =self.conc_code
                )
            )
        return

class Taught(object):
    def __init__(self, course_inst, prof_name):
        self.year = course_inst.start_year
        self.prof_name = prof_name
        self.course = course_inst.code
        self.session = course_inst.session

    def __repr__(self):
        return '<Taught {}->{} {}>'.format(
            self.prof_name,
            self.course,
            self.year
            )
    
    def merge(self):
        ""
        cypher = """
        MERGE (p:Prof {{name:'{name}'}}) -[t:Taught]-> (c:Course {{code:'{code}'}})
        ON CREATE SET t.years = [{year}]
        RETURN t.years
        """.format(
                   code=self.course,
                   name=self.prof_name,
                   year=self.year
                   )
        records = [i for i in self.session.run(cypher)]
        years = []
        for record in records:
            years += record['t.years']
        if self.year not in years:
            self.session.run(
                """
                MATCH (p:Prof) -[t:Taught]-> (c:Course)
                WHERE p.name = {name} AND c.code = {code}
                SET t.years = t.years + {year}
                """.format(
                    name=self.prof_name,
                    code=self.course,
                    year=self.year
                )
            )
        return
#%%
if __name__ == "__main__":
    if os.getcwd().split('/')[::-1][0] != 'BatesGraph':
        os.chdir('ProgrammingProjects/BatesGraph')
#        raise FileNotFoundError('Not in project directory')
    if 'USR' not in dir():
        USR = input('username: ')
        PWD = input('password: ')
    DRIVER = GraphDatabase.driver(
        "bolt://localhost",
        auth=basic_auth(USR, PWD)
    )
    SESSION = DRIVER.session()
    SESSION.run('CREATE CONSTRAINT ON (c:Course) ASSERT c.code IS UNIQUE')
    SESSION.run('CREATE CONSTRAINT ON (d:Dept) ASSERT d.code IS UNIQUE')
    SESSION.run('CREATE CONSTRAINT ON (p:Prof) ASSERT p.name IS UNIQUE')
    SESSION.run('CREATE CONSTRAINT ON (p:Program) ASSERT p.code IS UNIQUE')
    BATES = Bates()
    #%%
    PROG = pb.ProgressBar()    
    for dept, year in PROG(BATES.page_query_tuples:
        PAGE = Page(dept, year)
#%%
p = Page('&a=renderDept&d=WGST', '?s=1097')
p.parse()
c = p.courses[0]
print(c)
print(c.parse_professors())
#%%
c.merge_profs()
#%%
print(c.parse_requirements())
#%%
PROG = pb.ProgressBar()
for dept, year in PROG(BATES.page_query_tuples):
    PAGE = Page(dept, year)