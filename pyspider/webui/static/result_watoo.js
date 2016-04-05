// vim: set et sw=2 ts=2 sts=2 ff=unix fenc=utf8:
// Author: xiyoulaoyuanjia


$(function() {

  // onload
  function fill_progress(project, type, info) {
    var $e = $("tr[data-name="+project+"] td.progress-"+type);
    if (!!!info) {
      $e.attr("title", "");
      $e.attr('data-value', 0);
      $e.find(".progress-text").text(type);
      $e.find(".progress-pending").width("0%");
      $e.find(".progress-success").width("0%");
      $e.find(".progress-retry").width("0%");
      $e.find(".progress-failed").width("0%");
      return ;
    }

    var pending = info.pending || 0,
    success = info.success || 0,
    retry = info.retry || 0,
    failed = info.failed || 0,
    sum = info.task || pending + success + retry + failed;
    console.log(info)
    $e.attr("title", ""+type+" of "+sum+" tasks:\n"
            +(type == "all"
              ? "pending("+(pending/sum*100).toFixed(1)+"%): \t"+pending+"\n"
              : "new("+(pending/sum*100).toFixed(1)+"%): \t\t"+pending+"\n")
              +"success("+(success/sum*100).toFixed(1)+"%): \t"+success+"\n"
              +"retry("+(retry/sum*100).toFixed(1)+"%): \t"+retry+"\n"
              +"failed("+(failed/sum*100).toFixed(1)+"%): \t"+failed
           );
           $e.attr('data-value', sum);
           $e.find(".progress-text").text(type+": "+sum);
           $e.find(".progress-pending").width(""+(pending/sum*100)+"%");
           $e.find(".progress-success").width(""+(success/sum*100)+"%");
           $e.find(".progress-retry").width(""+(retry/sum*100)+"%");
           $e.find(".progress-failed").width(""+(failed/sum*100)+"%");
  }


  function update_counters() {
    $.get('/results/counter', function(data) {
      //console.log(data);
      $('tr[data-name]').each(function(i, tr) {
        var project = $(tr).data('name');

        var info = data[project];
        if (info === undefined) {
          return ;
        }

        if (info['5m_time']) {
          var fetch_time = (info['5m_time']['fetch_time'] || 0) * 1000;
          var process_time = (info['5m_time']['process_time'] || 0) * 1000;
          $(tr).find('.project-time').attr('data-value', fetch_time+process_time).text(
            ''+fetch_time.toFixed(1)+'+'+process_time.toFixed(2)+'ms');
        } else {
          $(tr).find('.project-time').attr('data-value', '').text('');
        }



        fill_progress(project, '1day', info['1day']);
        fill_progress(project, '1week', info['1week']);
        fill_progress(project, '1month', info['1month']);
        fill_progress(project, '1year', info['1year']);
        fill_progress(project, 'all', info['all']);
      });
    });
  }

  window.setInterval(update_counters, 15*1000);
  update_counters();

  // table sortable
  Sortable.getColumnType = function(table, i) {
    var type = $($(table).find('th').get(i)).data('type');
    if (type == "num") {
      return Sortable.types.numeric;
    } else if (type == "date") {
      return Sortable.types.date;
    }
    return Sortable.types.alpha;
  };
  $('table.projects').attr('data-sortable', true);
  Sortable.init();
});
