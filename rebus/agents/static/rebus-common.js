$(document).ready(function() {
  var pathelems = location.pathname.split('/');
  if (pathelems.length > 1) {
  	navelem = $('ul.nav a[href^="/' + pathelems[1] + '"]').parent()
    if (navelem.length == 0) {
      if (pathelems[1] == "analysis") {
        title = "File analysis";
      } else if (pathelems[1] == "get") {
        title = pathelems[2] + " view";
      }
      $('ul.nav').prepend('<li class="active"><a href="' + location.href + '">' + title + '</a></li>')
    }
    navelem.addClass('active');
  }
});

$(function () {
    $('#fileupload').fileupload({
        dataType: 'json',
        add: function(e, data) {
            if (typeof updater !== 'undefined') {
              updater.reset();
            }
            $('#progress .progress-bar').css('width', '0%');
            $('.upload-status').text('Uploading file ' + data.files[0].name + '...');
            $('.upload-status-panel').show();
            data.submit();
        },
        dataType: 'json',
        done: function(e, data) {
            $('.upload-status').text('File ' + data.files[0].name + ' has successfully been uploaded.');
            $('#progress .progress-bar').css('width', '100%');
            $('.upload-status-panel').delay(2000).hide(200);
            if (location.pathname.split('/')[1] === "analysis") {
              updater.uuid = data.result.uuid;
              history.pushState({}, '', ['/analysis', updater.domain,
                  updater.uuid].join('/'));
              updater.poll();
            } else {
              window.location.assign(['/analysis', 'default',
                  data.result.uuid].join('/'));
            }
        },
        progressall: function (e, data) {
            var progress = parseInt(data.loaded / data.total * 100, 10);
            $('#progress .progress-bar').css(
                'width',
                progress + '%'
                );
        }
    });
});

