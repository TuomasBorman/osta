<%!
    from pdoc.html_helpers import minify_css
%>
<%def name="homelink()" filter="minify_css">
    .homelink {
        display: block;
        font-size: 2em;
        font-weight: bold;
        color: #555;
        padding-bottom: .5em;
        border-bottom: 1px solid silver;
    }
    .homelink:hover {
        color: inherit;
    }
    .homelink img {
        max-width:60%;
        max-height: 20em;
        margin: auto;
        margin-bottom: .3em;
        display: block;
    }
</%def>

<style>${homelink()}</style>
<link rel="canonical" href="https://github.com/TuomasBorman/osta">
<link rel="icon" href="./logo/osta_logo.png">