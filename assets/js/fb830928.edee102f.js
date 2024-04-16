"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[2685],{3905:(e,t,n)=>{n.d(t,{Zo:()=>c,kt:()=>b});var r=n(7294);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function a(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function s(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?a(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function i(e,t){if(null==e)return{};var n,r,o=function(e,t){if(null==e)return{};var n,r,o={},a=Object.keys(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var l=r.createContext({}),p=function(e){var t=r.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):s(s({},t),e)),n},c=function(e){var t=p(e.components);return r.createElement(l.Provider,{value:t},e.children)},u="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var n=e.components,o=e.mdxType,a=e.originalType,l=e.parentName,c=i(e,["components","mdxType","originalType","parentName"]),u=p(n),m=o,b=u["".concat(l,".").concat(m)]||u[m]||d[m]||a;return n?r.createElement(b,s(s({ref:t},c),{},{components:n})):r.createElement(b,s({ref:t},c))}));function b(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=n.length,s=new Array(a);s[0]=m;var i={};for(var l in t)hasOwnProperty.call(t,l)&&(i[l]=t[l]);i.originalType=e,i[u]="string"==typeof e?e:o,s[1]=i;for(var p=2;p<a;p++)s[p]=n[p];return r.createElement.apply(null,s)}return r.createElement.apply(null,n)}m.displayName="MDXCreateElement"},3460:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>s,default:()=>d,frontMatter:()=>a,metadata:()=>i,toc:()=>p});var r=n(7462),o=(n(7294),n(3905));const a={},s="Webhooks",i={unversionedId:"concepts/webhook",id:"concepts/webhook",title:"Webhooks",description:"Overview:",source:"@site/docs/concepts/webhook.md",sourceDirName:"concepts",slug:"/concepts/webhook",permalink:"/optimus/docs/concepts/webhook",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/concepts/webhook.md",tags:[],version:"current",lastUpdatedBy:"Yash Bhardwaj",lastUpdatedAt:1713251060,formattedLastUpdatedAt:"Apr 16, 2024",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Replay & Backup",permalink:"/optimus/docs/concepts/replay-and-backup"},next:{title:"Server Configuration",permalink:"/optimus/docs/server-guide/configuration"}},l={},p=[{value:"Overview:",id:"overview",level:2},{value:"Scope:",id:"scope",level:2},{value:"On Events:",id:"on-events",level:3},{value:"WebHook-Request:",id:"webhook-request",level:2},{value:"Method:",id:"method",level:3}],c={toc:p},u="wrapper";function d(e){let{components:t,...n}=e;return(0,o.kt)(u,(0,r.Z)({},c,n,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"webhooks"},"Webhooks"),(0,o.kt)("h2",{id:"overview"},"Overview:"),(0,o.kt)("p",null,"For Data Pipelines, there is often a requirement that systems external to Optimus expect notification/nudge upon job run state change. This helps people trigger external routines."),(0,o.kt)("h2",{id:"scope"},"Scope:"),(0,o.kt)("p",null,"Only HTTP/HTTPS webhooks are supported."),(0,o.kt)("h3",{id:"on-events"},"On Events:"),(0,o.kt)("table",null,(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Event"),(0,o.kt)("th",{parentName:"tr",align:null},"Optimus Event Category"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"Job Fail"),(0,o.kt)("td",{parentName:"tr",align:null},"'failure'")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"Job Success"),(0,o.kt)("td",{parentName:"tr",align:null},"'success'")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"Job SLA Miss"),(0,o.kt)("td",{parentName:"tr",align:null},"'sla_miss'")))),(0,o.kt)("h2",{id:"webhook-request"},"WebHook-Request:"),(0,o.kt)("h3",{id:"method"},"Method:"),(0,o.kt)("blockquote",null,(0,o.kt)("p",{parentName:"blockquote"},"HTTP POST"),(0,o.kt)("h3",{parentName:"blockquote",id:"url"},"URL:"),(0,o.kt)("p",{parentName:"blockquote"},"User configured"),(0,o.kt)("h3",{parentName:"blockquote",id:"request-content-type"},"Request Content-Type:"),(0,o.kt)("p",{parentName:"blockquote"},"application/json"),(0,o.kt)("h3",{parentName:"blockquote",id:"request-body"},"Request Body:"),(0,o.kt)("pre",{parentName:"blockquote"},(0,o.kt)("code",{parentName:"pre",className:"language-json"},'{\n   "job"            : "string",\n   "project"        : "string",\n   "namespace"      : "string",\n   "destination"    : "string",\n   "scheduled_at"   : "time-string:UTC",\n   "status"         : "string",\n   "job_label": {\n       "key"    : "value",\n       "key1"   : "value",\n       "key2"   : "value"\n   }\n}\n'))),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"\n### Expected Response\n- 200\n\n### On Error\n- Log it in Optimus Server\n\n\n## Add WebHooks in Job Specification\n```yaml\nversion: 1\nname: <JOB_NAME>\nowner: <OWNER>\nschedule:\n  start_date: \"2021-09-01\"\n  interval: 0 5 * * *\nbehavior:\n  webhook:\n    - on: success\n      endpoints:\n        - url: http://sub-domain.domain.com/path/to/the/webhook?some=somethingStatic\n          headers:\n            auth-header: '{{.secret.WEBHOOK_SECRET}}'\n            some_header: 'dummy value'\n    - on: sla_miss\n      endpoints:\n        - url: http://sub-domain.domain.com/path/to/the/webhook?some=somethingStatic\n    - on: failure\n      endpoints:\n        - url: http://sub-domain.domain.com/path/to/the/webhook1\n          headers:\n            auth: 'bearer: {{.secret.WEBHOOK_SECRET}}'\nnotify:\n  - on: failure\n    channels:\n      - slack://#somechannel\n      - pagerduty://#somechannel\ntask:\n  name: <task_name>\n  config:\n    labels:\n      label_name: <some_label>\ndependencies: []\n")))}d.isMDXComponent=!0}}]);