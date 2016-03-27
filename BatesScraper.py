# -*- coding: utf-8 -*-
"""
Created on Thu Dec 24 12:27:32 2015

@author: steven
"""

# 
# import block
import urllib3 as ul
ul.disable_warnings()
from lxml import html
http = ul.PoolManager()
from io import BytesIO
import igraph as ig
from collections import OrderedDict
import json
import shutil
import os
from random import randint

# extra tools
#def ExploreElement(e):
#    """
#    take an element from an ElementTree and Pretty Print its children
#    Args:
#    e: ElementTree element
#    """
#    print(html.etree.tostring(e, pretty_print=True).decode('UTF-8'))
    
def get_page(url):
    """
    takes a URL and returns an ElmentTree object
    Args:
    url: url string
    """
    page = http.request("GET", url).data
    tree = html.parse(BytesIO(page))
    return(tree)
    
# main functions in approximate order of their usage
def get_dept_urls():
    x1 = '//*[@id="deptList"]/div/li/a/@href'
    url = 'http://www.bates.edu/catalog/?s=current&a=renderStatic&c=courses'
    tree = get_page(url)
    url_list = ['www.bates.edu/catalog/' + x for x in tree.xpath(x1)]
    # add last year's classes
    url_list += [x.replace("s=current", "s=1000" ) for x in url_list]
    return(url_list)


def get_all_available_courses(url_list):
    """
    scrapes course information from the bates website;
    Args:
        url_list: a  list of the urls of each department's course catalog
    Returns:
        a 3-tuple of the dict mapping course codes to course info, the list
        of tuples (prereq, course), and a list of courses that appears more
        than once
    """
    x5 = './span[@class="CourseDesc"]/text()'

    # a dictionary mapping course codes to a url, their course descriptions,
    # and a list of prerequisites
    courses = OrderedDict()
    # a list of tuples of (prereq, course) codes
    el = []
    # a list of course codes that appear more than once
    crosslistings = []
    
    for url in url_list:
        tree = get_page(url)
        courseList = tree.xpath('//*[@id="col0"]/div[@class="Course"]')
        for course in courseList:
            name = course.xpath('./h4[@class="crsname"]/text()')[0]
            code = name.split('.')[0]
            url_temp = url + '#' + course.xpath('./a[2]/@name')[0]
            if code in courses.keys():
                # the most recent course details have been entered
                crosslistings.append(url_temp)
            else:
                # this is a new course to the list of courses
                concentrations = course.xpath('./span/div/ul/li/a/text()')
                desc = ' '.join(course.xpath(x5))
                courses[code] = OrderedDict()
                courses[code]["name"] = name
                courses[code]["concentrations"] = concentrations
                courses[code]["description"] = desc
                courses[code]["url"] = "<a href='https://" + url_temp + \
                                        "'> LINK </a>"
                if "departments" not in courses[code].keys():
                    courses[code]["departments"] = [url.split('Dept&d=')[1]]
                else:
                    courses[code]["departments"] += [url.split('Dept&d=')[1]]
                courses[code]["prereqs"] =[]
                if 'Prerequisite(s):' in desc:
                    current = code.split()[0]
                    reqs = desc.split('Prerequisite(s):')[1]
                    courses[code]["prereqs"] = reqs
                    immediate_reqs = reqs.split('.')[0].split()
                    for w in immediate_reqs:
                        if w.isupper():
                            current = w
                        if w.isnumeric():
                            el.append((current + ' ' + w, code))
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
