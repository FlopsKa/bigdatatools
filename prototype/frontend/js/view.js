var mainTextField = {
  listener: {},

  start: function(wordlist) {
    var output = this.addSpanTags(wordlist);

    $('#speedtest').html("<div class=\"task\">" + output + "</div>");
    $('#speedtest').append("<div class=\"word_input\">" +
                           "<form class=\"input-group\"><input onkeyup=\"listener.inputChangedEvent()\" class=\"form-control speedtest_input\" /><span class=\"timer input-group-addon\">60</span>" +
                           "</form><p class=\"hint\"><small><i>Hint: Press \'Ctrl\' + \'R\' or &lt;Enter&gt; to restart.</i></small></p></div>");
    $('span[wordnum=0]').addClass('currentword');
    $('.speedtest_input').focus();
  },

  register: function(observer) {
    console.log("register called");
    listener = observer;
  },

  getInputFieldValue: function() {
    var input_field = $('.speedtest_input');
    return input_field.val();
  },

  advanceHighlightToNextWord: function() {
    var highlighted = $('.currentword');
    highlighted.removeClass('currentword');
    var next = highlighted.next('span');
    next.addClass('currentword');

    // move the lines upwards when the cursor jumps to the next line
    if(highlighted.offset().top != next.offset().top) {
      // delete the previous spans
      for(var i = 0; i <= highlighted.attr("wordnum"); i++) {
        $("[wordnum='" + i + "']").remove();
      }
    }
  },

  setColorOfActiveWordToRed: function() {
    var higlighted = $('.currentword');
    higlighted.addClass("wrong");
    higlighted.removeClass("right");
  },

  setColorOfActiveWordToGreen: function() {
    var higlighted = $('.currentword');
    higlighted.addClass("right");
    higlighted.removeClass("wrong");
  },

  clearInput: function() {
    var input_field = $('.speedtest_input');
    input_field.val('');
  },

  // private
  addSpanTags: function(words_without_span) {
    var output = "";
    for(var i = 0; i < words_without_span.length; i++) {
      output += "<span class=\"\" wordid=\"" + words_without_span[i].id + "\" wordnum=\"" + i + "\">" + words_without_span[i].word + "</span> ";
    }
    return output;
  }

}
