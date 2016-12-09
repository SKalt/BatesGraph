# -*- coding: utf-8 -*-
"""
Created on Thu Dec 24 12:27:32 2015

@author: steven
"""

# import block
from lxml import html
import os
import neo4j
import sqlite3
import sqlalchemy
from sqlalchemy import Column, Integer, SmallInteger, String
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
# class Bates(object):
#     "A class to organize the basic scraping functions"
#     

def get_years():
    """
    Gets the urls of splash pages for all years after 2013-14

    Returns:
        A list of strings in the format '?s=query' indicating catalog years
    """
    page = html.parse('http://www.bates.edu/catalog/?a=catalogList')
    links = page.xpath('//*[@id="catmenu"]//a')
    results = []
    for a in links:
        year = int(a.text.strip()[:4])
        if year > 2012:
            results.append(a.attrib['href'])
    return results

def map_years():
    """
    returns a dict mapping urlencoded year queries to academic year strs.

    Returns:
        as above.
    """
    page = html.parse('http://www.bates.edu/catalog/?s=1000&a=catalogList')
    links = page.xpath('//*[@id="catmenu"]//a')
    string_map = {i.attrib['href']:i.text[:9] for i in links}
    int_map = {}
    for key in string_map:
        int_map[key] = (int(i) for i in string_map[key].split('-'))
    return int_map
    
# main functions in approximate order of their usage
def get_dept_extensions():
    """
    Returns a urlencoded queries specifying departments
    """
    find_links = '//*[@id="deptList"]/div/li/a/@href'
    url = 'http://www.bates.edu/catalog/?s=current'
    tree = html.parse(url)
    return [href.replace('?s=current', '') for href in tree.xpath(find_links)]
    
#%%
def generate_dept_pages():
    """
    Generates a complete list of urls to scrape.

    Returns:
        a list of all department catalog URLs to scrape. Some may not exist,
        since new departments have been added after the minimum year of 
        get_years().
    """
    years = get_years()
    dept_extensions = get_dept_extensions()
    results = []
    for year in years:
        for dept in dept_extensions:
            results.append('http://www.bates.edu/catalog/' + year + dept)
    return results
#%%

#%%
def get_sqlite_db_connection():
    engine = sqlalchemy.create_engine('sqlite+pysqlite:///bates.db')
    return engine
    # cur = conn.cursor()
    # def commit(command):
    #     cur.execute(command)
    #     conn.commit()
    
    # commit(
    #     """
    #     CREATE TABLE IF NOT EXISTS page_status(
    #         url varchar(100),
    #         status char(3),
    #     )
    #     """
    # )
    # commit(
    #     """
    #     CREATE TABLE IF NOT EXISTS courses(
    #         code varchar(10),
    #         description varchar
    #     )
    #     """
    # )
    # commit(
    #     """
    #     CREATE TABLE IF NOT EXISTS professors(
    #     name varchar(50),
    #     code varchar(10),
    #     year integer
    # return conn, cur

class Page(Base):
    """
    Holds functions for scraping a departmental course catalog page.
    """
    def map_codes():
        """
        Returns a dict mapping all department names to department codes and another
        dict mapping all interdisciplinary department shortcodes to department
        codes.

        Returns:
            At tuple of two dicts, DEPT:DT and Dept Name:DEPT.
        """
        root = html.parse('http://www.bates.edu/catalog/').getroot()
        subj_name = root.xpath(".//div[@class='subjName']//li/text()")
        if not subj_name:
            raise ValueError('No results; check xpath')
        subj_name = [i.replace('and', '&') for i in subj_name]
        subj_code = root.xpath(".//div[@class='subjCode']//li/text()")
        subj_code_2 = root.xpath(".//div[@class='subjCodeInt']//li/text()")
        if not len(subj_code) == len(subj_code_2) == len(subj_name):
            raise ValueError('unequal-length code lists')
        else:
            shortcode_map = {}
            names_map = {}
            for i in range(len(subj_name)):
                names_map[subj_name[i]] = subj_code[i]
                shortcode_map[subj_code_2[i]] = subj_code[i]
            return shortcode_map, names_map
    
    dept_shortcode_map, dept_name_map = map_codes()
    name_dept_map = {v:k for k, v in dept_name_map}
    year_map = map_years()

    def scrape_page(self, neo4j_session):
        """
        Scrape all courses on a Bates department course catalog page.

        Args:
            url: a string url of a Bates departmental catalog page.
            session: a neo4j.GraphDatabase.driver.session object.

        Returns:
            None.
        """
        courses = self.page.xpath('//*[@class="Course"]')
        for course in courses:
            scrape_course(course, neo4j_session)
    
    def get_years(self):
        """
        Maps the urlencoded catalog year to a tuple (start_year, end_year)

        Returns:
            a tuple of catalog coverage: (start_year, end_year). The start year
            corresponds to the falls semester year, and the end year corresponds
            to the spring semester year.
        """
        year = year_map[self.url.split('?s=')[1].split('&')[0]]
        years = years_map[year]
        return years
        
    def __init__(self, url, neo4j_session):
        self.url = url
        self.dept_code = url.split('&a=renderDept&d=')[1]
        self.start_year, self.end_year = self.get_years()
        self.page = html.parse(url)
        self.raw_courses = self.page.xpath('//*[@class="Course"]')
        if not self.raw_courses:
            # no courses found, a 404 page
            self.status = 404
            self.dept_name = 'Does not exist at this time'
            
        else:
            self.status = 200
            
            self.dept_name = name_dept_map[self.dept_code]
            self.selftart_year, self.end_year = years
            
        
        
        else:
            self.status = 200
        self.courses = [Course(url, course) for course in self.raw_courses]
    
    url = Column(String())
    dept = Column(String(4))
    start_year = Column(SmallInteger())
    end_year = Column(SmallInteger())
    status = Column(String(10))
    
    
class Course(Page):
    "Inherets from Page to keep the year_, dept_map s"
    def __init__(self, url, course_div):
        self.url = url
        self.course_div = course_div
    
    def scrape_course(self, session):
        """
        Put course info into a this Course object.
        Args:
            course: a lxml.html._Element representing a div containing course
                details.
            url: the url of the catalog page.
            session: a neo4j.GraphDatabase.driver.session object.
        Returns:
            None
        """
        self.name = self.course_div.xpath('./h4[@class="crsname"]/text()')[0]
        self.code = self.name.split('.')[0]
        self.link =  self.url + '#' + self.course_div.xpath('./a[2]/@name')[0]
        self.depts = []
        for code in self.code.split()[0].split('/'):
            if code in self.dept_map:
                code = self.dept_map[code]
            self.depts.append(code)
        self.concentrations = course.xpath('./span/div/ul/li/a/text()')
        self.desc = self.course_div.xpath('./span[@class="CourseDesc"]/text()')
        self.desc = ' '.join(self.desc)
        if 'Prerequisite(s):' in self.desc:
            self.has_requirements = True
            reqs = desc.split('Prerequisite(s):')[1].split('.')[0].strip()
            
    def parse_requirements(self):
        reqs = desc.split('Prerequisite(s):')[1].split('.')[0].strip()
        def comma_split():
            
#                    immediate_reqs = reqs.split()
#                    for w in immediate_reqs:
#                        if w.isupper():
#                            current = w
#                        if w.isnumeric():
#                            el.append((current + ' ' + w, code))
                            # req -> course
    return((courses, el, crosslistings))

def get_el(course_dict):
    el = []
    count = len(course_dict)
    for code in course_dict:
        print(count)
        count -= 1
        desc = course_dict[code]["description"]
        if 'Prerequisite(s):' in desc:
            current = code.split()[0]
            reqs = desc.split('Prerequisite(s):')[1]
            course_dict[code]["prereqs"] = reqs
            immediate_reqs = reqs.split('.')[0].split()
            for w in immediate_reqs:
                if w.isupper():
                    current = w
                if w.isnumeric():
                    el.append((current + ' ' + w, code))
                    # req -> course
    return(el)

def make_course_graph(courses, el):
    """
    extracts prerequisites from course descriptions to make a directed 
    igraph object with courses as nodes and edges from prerequisites to courses
    """
    extras = [j for i,j in el if j not in courses.keys()]
    extras += [i for i,j in el if i not in courses.keys()]
    g = ig.Graph(len(courses) + len(extras))
    g.vs["name"] = list(courses.keys()) + extras
    g.add_edges(el)
    return(g)
    
def make_sugiyama_layout(g):
    lyt = g.layout_sugiyama(hgap=10, vgap=10, maxiter=1000)
    return(lyt)

def export_all_json(all_course_details):
    f = open('Bates.json', 'w')
    f.write(json.dumps(all_course_details, separators=(',', ':')))
    f.close()
    

def subset_courses(dept, course_details_dict, g):
    """
    takes a string of a department code such as 'SOC' and selects all
    listed courses and the courses they require or those that require them.
    """
    # get a list of all the course codes in the department    courses = []
    codes = []    
    for i in course_details_dict:
        if dept in course_details_dict[i]["departments"]:
            codes.append(i)
    # get all related courses
    vertexes = [i for i in g.vs if i["name"] in codes]
    neighbors = [v for neighbs in g.neighborhood(vertexes) for v in neighbs]
    neighbors = list(set(neighbors))
    subG = g.subgraph(neighbors)
    return(subG)

def find_or_make_directory_address(dept_string):
    """
    finds whether there is a directory named after a deptarment string, and
    if not, makes one
    Args:
        dept_string: a string specifying a department at an institution
    Returns:
        a string specifying an directory
    """
    directory = './{}'.format(dept_string)
    if not os.path.exists(directory):
        shutil.copytree('../AmherstGraph/NetworkTemplateWithoutData_JSON',
                        directory)
    return directory
   
def get_rgb():
    "returns a tuple of three numbers between 0 and 255"
    return ((randint(0, 255), randint(0, 255), randint(0, 255)))

def make_json(dept_string, course_details, complete_course_graph):
    """
    This function makes a JSON object called 'data', to be inserted
    into the directory exported by a sigma.js template to
    make an interactive web visualization of the prereqs network
    Args:
        dept_string: a string specifying a department at an institution
        course_details: a dict mapping course codes to course details at an
            institution
        complete_course_graph: a directed igraph object containing all the
            courses at the institution as nodes and all the requirements of
            each course as the first-order in-neigbhors of the course
    Returns:
        data: a  JSON file specifying the nodes and edges to be drawn by
            sigma.js and the information about nodes to display.
    """
    data = {"edges":[], "nodes":[]}

    #get the subgraph, node positions
    subgraph = subset_courses(dept_string, course_details, 
                              complete_course_graph)
    sugiyama_layout = make_sugiyama_layout(subgraph)

    unique_departments = [name.split()[0] for name in subgraph.vs["name"]]
    department_colors = {dept:get_rgb() for dept in unique_departments}

    for node in enumerate(subgraph.vs["name"]):
        if node[1] in course_details.keys():
            node_output = OrderedDict()
            node_output["label"] = node[1]
            node_output["x"] = sugiyama_layout[node[0]][0]
            node_output["y"] = sugiyama_layout[node[0]][1]
            node_output["id"] = str(node[0])

            attrs = OrderedDict()
            attrs["Title"] = course_details[node[1]]["name"]
            attrs["Description"] = course_details[node[1]]["description"]
            attrs["Department Code"] = node[1].split()[0]
            attrs["Course Site"] = course_details[node[1]]["url"]
            attrs["Requisite"] = course_details[node[1]]["prereqs"]
            attrs["Concentrations"] = course_details[node[1]]["concentrations"]
            node_output["attributes"] = attrs
            node_output["color"] = 'rgb' + \
                str(department_colors[node[1].split()[0]])
            node_output["size"] = 10.0
        # if the course has no retrieved details:
        else:
            node_output = OrderedDict()
            node_output["label"] = node[1]
            node_output["x"] = sugiyama_layout[node[0]][0]
            node_output["y"] = sugiyama_layout[node[0]][1]
            node_output["id"] = str(node[0])
            node_output["attributes"] = OrderedDict()
            node_output["attributes"]["Title"] = node[1]
            node_output["attributes"]["Description"] = 'not offered in the' + \
                " last 4 semesters"
            node_output["attributes"]["Department Code"] = (node[1].split()[0])
            node_output["attributes"]["Course Site"] = ""
            node_output["attributes"]["Requisite"] = ''
            node_output["attributes"]["Concentrations"] = []
            node_output["color"] = 'rgb' + \
                str(department_colors[node[1].split()[0]])
            node_output["size"] = 10.0
        data["nodes"].append(node_output)

    edgelist = subgraph.get_edgelist()
    for edge in enumerate(edgelist):
        color = department_colors[subgraph.vs["name"][edge[1][1]].split()[0]]
        color = 'rgb' + str(color)
        edge_output = OrderedDict()
        edge_output["label"] = ''
        edge_output["source"] = str(edge[1][0])
        edge_output["target"] = str(edge[1][1])
        edge_output["id"] = str(len(node_output) - 1 + 2*edge[0])
        #                                        ^ this is to conform with the
        # odd indexing I see in working visualisations
        edge_output["attributes"] = {}
        edge_output["color"] = color # target node color
        edge_output["size"] = 1.0
        data["edges"].append(edge_output)
    return data

def export_json(dept_string, course_details, complete_course_graph):
    """
    writes the data json object describing a major's prerequisite network to
    a file called 'data.json' in a directory named after the department
    Args:
        inst_code: U for UMass, A for Amherst, etc.
        dept_string: a string specifying a department at an institution
        course_details: a dict mapping course codes to course details at an
            institution
    Returns:
        None
    """
    data = make_json(dept_string, course_details, complete_course_graph)
    path = find_or_make_directory_address(dept_string)
    path += '/data.json'
    json_file = json.dumps(data, separators=(',', ':'))
    target_file = open(path, 'w')
    target_file.write(json_file)
    target_file.close()
#%%
if __name__ == "__main__":
#    (COURSES, EL, XLISTINGS) = get_all_available_courses(get_dept_urls())
#    export_all_json(COURSES)
    TEMP = open('Bates.json')
    COURSES = json.loads(TEMP.read())
    TEMP.close()
    EL = get_el(COURSES)
    # TODO: rewrite to overwrite last semester
    G = make_course_graph(COURSES, EL)
    DEPTS = []
    #%%
for code in COURSES:
    DEPTS += COURSES[code]["departments"]
DEPTS = list(set(DEPTS))
for dept in DEPTS:
    print(dept)
    export_json(dept, COURSES, G)
