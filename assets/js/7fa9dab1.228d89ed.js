"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[9047],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>h});var a=n(7294);function i(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function r(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){i(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,a,i=function(e,t){if(null==e)return{};var n,a,i={},o=Object.keys(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||(i[n]=e[n]);return i}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(i[n]=e[n])}return i}var s=a.createContext({}),d=function(e){var t=a.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):r(r({},t),e)),n},p=function(e){var t=d(e.components);return a.createElement(s.Provider,{value:t},e.children)},u="mdxType",c={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var n=e.components,i=e.mdxType,o=e.originalType,s=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),u=d(n),m=i,h=u["".concat(s,".").concat(m)]||u[m]||c[m]||o;return n?a.createElement(h,r(r({ref:t},p),{},{components:n})):a.createElement(h,r({ref:t},p))}));function h(e,t){var n=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var o=n.length,r=new Array(o);r[0]=m;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[u]="string"==typeof e?e:i,r[1]=l;for(var d=2;d<o;d++)r[d]=n[d];return a.createElement.apply(null,r)}return a.createElement.apply(null,n)}m.displayName="MDXCreateElement"},8068:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>r,default:()=>c,frontMatter:()=>o,metadata:()=>l,toc:()=>d});var a=n(7462),i=(n(7294),n(3905));const o={},r="Intervals and Windows",l={unversionedId:"concepts/intervals-and-windows",id:"concepts/intervals-and-windows",title:"Intervals and Windows",description:"When defining a new job, you need to define the interval (cron) at which it will be triggered. This parameter can give",source:"@site/docs/concepts/intervals-and-windows.md",sourceDirName:"concepts",slug:"/concepts/intervals-and-windows",permalink:"/optimus/docs/concepts/intervals-and-windows",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/concepts/intervals-and-windows.md",tags:[],version:"current",lastUpdatedBy:"Arinda Arif",lastUpdatedAt:1714609152,formattedLastUpdatedAt:"May 2, 2024",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Macros",permalink:"/optimus/docs/concepts/macros"},next:{title:"Secret",permalink:"/optimus/docs/concepts/secret"}},s={},d=[{value:"Window Configuration",id:"window-configuration",level:2},{value:"Window configuration version 1 and 2",id:"window-configuration-version-1-and-2",level:3},{value:"Custom Window",id:"custom-window",level:3},{value:"Window Preset (since v0.10.0)",id:"window-preset-since-v0100",level:3}],p={toc:d},u="wrapper";function c(e){let{components:t,...n}=e;return(0,i.kt)(u,(0,a.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"intervals-and-windows"},"Intervals and Windows"),(0,i.kt)("p",null,"When defining a new job, you need to define the ",(0,i.kt)("strong",{parentName:"p"},"interval (cron)")," at which it will be triggered. This parameter can give\nyou a precise value when the job is scheduled for execution but only a rough estimate exactly when the job is executing.\nIt is very common in a ETL pipeline to know when the job is exactly executing as well as for what time window the current\ntransformation will consume the data."),(0,i.kt)("p",null,"For example, assume there is a job that querying from a table using below statement:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-sql"},"SELECT * FROM table WHERE\ncreated_at >= '{{.START_DATE}}' AND created_at < '{{.END_DATE}}'\n")),(0,i.kt)("p",null,(0,i.kt)("strong",{parentName:"p"},"START_DATE")," and ",(0,i.kt)("strong",{parentName:"p"},"END_DATE")," could be replaced at the time of compilation with based on its window configuration.\nWithout the provided filter, we will have to consume all the records which are created till date inside the table\neven though the previous rows might already been processed."),(0,i.kt)("p",null,"The ",(0,i.kt)("em",{parentName:"p"},"DSTART")," and ",(0,i.kt)("em",{parentName:"p"},"DEND")," values of the input window could vary depending on the ETL job requirement."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"For a simple transformation job executing daily, it would need to consume full day work of yesterday\u2019s data."),(0,i.kt)("li",{parentName:"ul"},"A job might be consuming data for a week/month for an aggregation job, but the data boundaries should be complete,\nnot consuming any partial data of a day.")),(0,i.kt)("h2",{id:"window-configuration"},"Window Configuration"),(0,i.kt)("p",null,"Optimus allows user to define the amount of data window to consume through window configurations. The configurations\nact on the schedule",(0,i.kt)("em",{parentName:"p"},"time of the job and applied in order to compute _DSTART")," and ",(0,i.kt)("em",{parentName:"p"},"DEND"),"."),(0,i.kt)("p",null,"The following is the list of available configuration the user can setup a window:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Size"),': size enables the user to define the duration for which the data needs to be consumed by job. Size can be defined in\nin units like "1h", "1d", "1w", "1M" to define the respective size of data to consume.'),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Shift By"),": optional configuration to allow shifting the window by the specified value, e.g., a configuration with size 1d, with\nshift by -1d, means that it will consume data of 1 day, the day before yesterday."),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Truncate_to"),': optional configuration to override the time unit for the window interval, e.g., a config with size 1d, with\ntruncate_to "h", will mean data for last 24 hours, to the end of previous hour.'),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Location"),": optional configuration to define the time zone to be used for this window configuration, if not defined the\ndefault value of location will be UTC.")),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-yaml"},'window:\n  size: 1d\n  shift_by: -1d\n  truncate_to: "h"\n  location: "Asia/Jakarta"\n')),(0,i.kt)("p",null,"Will provide output for reference time ",(0,i.kt)("inlineCode",{parentName:"p"},"2023-12-01T03:00:00Z")," (2023-12-01T10:00:00+07:00)"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"DEND:'2023-11-30T10:00:00+07:00'"),(0,i.kt)("li",{parentName:"ul"},"END_DATE: '2023-11-30'"),(0,i.kt)("li",{parentName:"ul"},"START_DATE='2023-11-29'"),(0,i.kt)("li",{parentName:"ul"},"DSTART: '2023-11-29T10:00:00+07:00'")),(0,i.kt)("h3",{id:"window-configuration-version-1-and-2"},"Window configuration version 1 and 2"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Truncate_to"),': The data window on most of the scenarios needs to be aligned to a well-defined time window\nlike month start to month end, or week start to weekend with week start being monday, or a complete day.\nInorder to achieve that the truncate_to option is provided which can be configured with either of these values\n"h", "d", "w", "M" through which for a given schedule_time the end_time will be the end of last hour, day, week, month respectively.'),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Offset"),": Offset is time duration configuration which enables user to move the ",(0,i.kt)("inlineCode",{parentName:"li"},"end_time"),' post truncation.\nUser can define the duration like "24h", "2h45m", "60s", "-45m24h", "0", "", "2M", "45M24h", "45M24h30m"\nwhere "h","m","s","M" means hour, month, seconds, Month respectively.'),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Size"),": Size enables user to define the amount of data to consume from the ",(0,i.kt)("inlineCode",{parentName:"li"},"end_time")," again defined through the duration same as offset.")),(0,i.kt)("p",null,"To further understand, the following is an example with its explanation. ",(0,i.kt)("strong",{parentName:"p"},"Important")," note, the following example uses\nwindow ",(0,i.kt)("inlineCode",{parentName:"p"},"version: 2")," because ",(0,i.kt)("inlineCode",{parentName:"p"},"version: 1")," will soon be deprecated."),(0,i.kt)("p",null,"For example, previous-mentioned job has ",(0,i.kt)("inlineCode",{parentName:"p"},"0 2 * * *")," schedule interval and is scheduled to run on\n",(0,i.kt)("strong",{parentName:"p"},"2023-03-07 at 02.00 UTC")," with following details:"),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:null},"Configuration"),(0,i.kt)("th",{parentName:"tr",align:null},"Value"),(0,i.kt)("th",{parentName:"tr",align:null},"Description"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"Truncate_to"),(0,i.kt)("td",{parentName:"tr",align:null},"d"),(0,i.kt)("td",{parentName:"tr",align:null},"Even though it is scheduled at 02.00 AM, data window will be day-truncated (00.00 AM).")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"Offset"),(0,i.kt)("td",{parentName:"tr",align:null},"-24h"),(0,i.kt)("td",{parentName:"tr",align:null},"Shifts the window to be 1 day earlier.")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"Size"),(0,i.kt)("td",{parentName:"tr",align:null},"24h"),(0,i.kt)("td",{parentName:"tr",align:null},"Gap between DSTART and DEND is 24h.")))),(0,i.kt)("p",null,"Above configuration will produce below window:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("em",{parentName:"li"},"DSTART"),": 2023-03-05T00:00:00Z"),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("em",{parentName:"li"},"DEND"),": 2023-03-06T00:00:00Z")),(0,i.kt)("p",null,"This means, the query will be compiled to the following query"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-sql"},"SELECT * FROM table WHERE\ncreated_at >= DATE('2023-03-05T00:00:00Z') AND\ncreated_at < DATE('2023-03-06T00:00:00Z')\n")),(0,i.kt)("p",null,"Assume the table content is as the following:"),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:null},"name"),(0,i.kt)("th",{parentName:"tr",align:null},"created_at"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"Rick"),(0,i.kt)("td",{parentName:"tr",align:null},"2023-03-05")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"Sanchez"),(0,i.kt)("td",{parentName:"tr",align:null},"2023-03-06")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"Serious"),(0,i.kt)("td",{parentName:"tr",align:null},"2023-03-07")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"Sam"),(0,i.kt)("td",{parentName:"tr",align:null},"2023-03-07")))),(0,i.kt)("p",null,"When the job that scheduled at ",(0,i.kt)("strong",{parentName:"p"},"2023-03-07")," runs, the job will consume ",(0,i.kt)("inlineCode",{parentName:"p"},"Rick")," as the input of the table."),(0,i.kt)("p",null,"Window configuration can be specified in two ways, through custom window configuration and through window preset."),(0,i.kt)("h3",{id:"custom-window"},"Custom Window"),(0,i.kt)("p",null,"Through this option, the user can directly configure the window that meets their requirement in the job spec YAML.\nThe following is an example of its usage:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-yaml"},"version: 3 # decides window version\nname: sample-project.playground.table1\nowner: sample_owner\nschedule:\n  ...\nbehavior:\n  ...\ntask:\n  name: bq2bq\n  config:\n    ...\n  window:\n    size: 1d\nlabels:\n  ...\nhooks: []\ndependencies: []\n")),(0,i.kt)("p",null,"Notice the window configuration is specified under field ",(0,i.kt)("inlineCode",{parentName:"p"},"task.window"),". ",(0,i.kt)("strong",{parentName:"p"},"Important")," note, the ",(0,i.kt)("inlineCode",{parentName:"p"},"version")," field decides which\nversion of window capability to be used. Currently available window version are ",(0,i.kt)("inlineCode",{parentName:"p"},"version: 1"),", ",(0,i.kt)("inlineCode",{parentName:"p"},"version: 2")," and ",(0,i.kt)("inlineCode",{parentName:"p"},"version: 3"),". Version 3 is recommended\nto be used as version 1 and version 2 will soon be deprecated. To play around with window configuration, try ",(0,i.kt)("inlineCode",{parentName:"p"},"playground")," feature for window:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-bash"},"# executes playground for v3\noptimus playground window\n\n# executes playground for v1 and v2\noptimus playground window --v1-v2\n")),(0,i.kt)("h3",{id:"window-preset-since-v0100"},"Window Preset (since v0.10.0)"),(0,i.kt)("p",null,"Window preset is a feature that allows easier setup of window configuration while also maintaining consistency. Through this feature,\nthe user can configure a definition of window once, then use it multiple times through the jobs which require it. ",(0,i.kt)("strong",{parentName:"p"},"Important")," note,\nwindow preset always use window ",(0,i.kt)("inlineCode",{parentName:"p"},"version: 2"),". The main components of window preset are as follow."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Window Preset File"))),(0,i.kt)("p",null,"Presets configuration is put in a dedicated YAML file. The way to configure it still uses the same window configuration\nlike ",(0,i.kt)("inlineCode",{parentName:"p"},"truncate_to"),", ",(0,i.kt)("inlineCode",{parentName:"p"},"shift_by"),", ",(0,i.kt)("inlineCode",{parentName:"p"},"location")," and ",(0,i.kt)("inlineCode",{parentName:"p"},"size"),". Though, there are some additions, like the name of the preset and the description to explain this preset.\nThe following is an example of how to define a preset under ",(0,i.kt)("inlineCode",{parentName:"p"},"presets.yaml")," file (note that the file name does not have to be this one)."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-yaml"},"presets:\n  yesterday:\n    description: defines yesterday window\n    window:\n      size: 1d\n  last_month:\n    description: defines last 30 days window\n    window:\n      size: 1M\n")),(0,i.kt)("p",null,"In the above example, the file ",(0,i.kt)("inlineCode",{parentName:"p"},"presets.yaml")," defines two presets, named ",(0,i.kt)("inlineCode",{parentName:"p"},"yesterday")," and ",(0,i.kt)("inlineCode",{parentName:"p"},"last_month"),". The name of preset ",(0,i.kt)("strong",{parentName:"p"},"SHOULD")," be\nin lower case. All of the fields are required, unless specified otherwise."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Preset Reference under Project"))),(0,i.kt)("p",null,"If the preset file is already specified, the next thing to do is to ensure that the preset file is referenced under project configuration.\nThe following is an example to refer the preset file under project configuration:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-yaml"},"version: 1\nlog:\n  ...\nhost: localhost:9100\nproject:\n  name: development_project\n  preset_path: ./preset.yaml # points to preset file\n  config:\n    ...\nnamespaces:\n  ...\n")),(0,i.kt)("p",null,"In the above example, a new field is present, named ",(0,i.kt)("inlineCode",{parentName:"p"},"preset_path"),". This path refers to where the preset file is located."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Preset Reference for Job Specification"))),(0,i.kt)("p",null,"Now, if the other two components are met, where the window preset file is specified and this file is referenced by the project, it means\nit is ready to be used. And the way to use it is by referencing which preset to be used in whichever job requires it. The following is an example\nof its usage:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-yaml"},"version: 1 # preset always use window version 3\nname: sample-project.playground.table1\nowner: sample_owner\nschedule:\n  ...\nbehavior:\n  ...\ntask:\n  name: bq2bq\n  config:\n    ...\n  window:\n    preset: yesterday\nlabels:\n  ...\nhooks: []\ndependencies: []\n")),(0,i.kt)("p",null,(0,i.kt)("strong",{parentName:"p"},"Important")," note, preset is optional in nature. It means that even if the preset is specified, the user can still use\nthe custom window configuration depending on their need."))}c.isMDXComponent=!0}}]);