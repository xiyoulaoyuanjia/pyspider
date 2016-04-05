#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<i@binux.me>
#         http://binux.me
# Created on 2014-10-19 16:23:55

from __future__ import unicode_literals

from flask import render_template, request, json
from flask import Response
from .app import app
from pyspider.libs import result_dump
import socket

@app.route('/results')
def result():
    resultdb = app.config['resultdb']
    project = request.args.get('project')
    offset = int(request.args.get('offset', 0))
    limit = int(request.args.get('limit', 20))

    count = resultdb.count(project)
    results = list(resultdb.select(project, offset=offset, limit=limit))

    return render_template(
        "result.html", count=count, results=results,
        result_formater=result_dump.result_formater,
        project=project, offset=offset, limit=limit, json=json
    )


@app.route('/results/dump/<project>.<_format>')
def dump_result(project, _format):
    resultdb = app.config['resultdb']
    # force update project list
    resultdb.get(project, 'any')
    if project not in resultdb.projects:
        return "no such project.", 404

    offset = int(request.args.get('offset', 0))
    limit = int(request.args.get('limit', 0))
    results = resultdb.select(project, offset=offset, limit=limit)

    if _format == 'json':
        valid = request.args.get('style', 'rows') == 'full'
        return Response(result_dump.dump_as_json(results, valid),
                        mimetype='application/json')
    elif _format == 'txt':
        return Response(result_dump.dump_as_txt(results),
                        mimetype='text/plain')
    elif _format == 'csv':
        return Response(result_dump.dump_as_csv(results),
                        mimetype='text/csv')

@app.route('/results/counter')
def results_counter():
      rpc = app.config['result_rpc']
      if rpc is None:
          return json.dumps({})

      result = {}
      try:
          for project, counter in rpc.counter('5m_time', 'avg').items():
              result.setdefault(project, {})['5m_time'] = counter
          for project, counter in rpc.counter('5m', 'sum').items():
              result.setdefault(project, {})['5m'] = counter
          for project, counter in rpc.counter('1h', 'sum').items():
              result.setdefault(project, {})['1h'] = counter
          for project, counter in rpc.counter('1day', 'sum').items():
              result.setdefault(project, {})['1day'] = counter
          for project, counter in rpc.counter('1week', 'sum').items():
              result.setdefault(project, {})['1week'] = counter
          for project, counter in rpc.counter('1month', 'sum').items():
              result.setdefault(project, {})['1month'] = counter
          for project, counter in rpc.counter('1year', 'sum').items():
              result.setdefault(project, {})['1year'] = counter
          for project, counter in rpc.counter('all', 'sum').items():
              result.setdefault(project, {})['all'] = counter
      except socket.error as e:
          app.logger.warning('connect to scheduler rpc error: %r', e)
          return json.dumps({}), 200, {'Content-Type': 'application/json'}

      return json.dumps(result), 200, {'Content-Type': 'application/json'}
        
