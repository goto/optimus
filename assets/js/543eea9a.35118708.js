"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[1986],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>g});var a=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function l(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function i(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},o=Object.keys(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var s=a.createContext({}),c=function(e){var t=a.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):l(l({},t),e)),n},p=function(e){var t=c(e.components);return a.createElement(s.Provider,{value:t},e.children)},u="mdxType",m={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},d=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,o=e.originalType,s=e.parentName,p=i(e,["components","mdxType","originalType","parentName"]),u=c(n),d=r,g=u["".concat(s,".").concat(d)]||u[d]||m[d]||o;return n?a.createElement(g,l(l({ref:t},p),{},{components:n})):a.createElement(g,l({ref:t},p))}));function g(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=n.length,l=new Array(o);l[0]=d;var i={};for(var s in t)hasOwnProperty.call(t,s)&&(i[s]=t[s]);i.originalType=e,i[u]="string"==typeof e?e:r,l[1]=i;for(var c=2;c<o;c++)l[c]=n[c];return a.createElement.apply(null,l)}return a.createElement.apply(null,n)}d.displayName="MDXCreateElement"},322:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>l,default:()=>m,frontMatter:()=>o,metadata:()=>i,toc:()=>c});var a=n(7462),r=(n(7294),n(3905));const o={},l="Manage BigQuery Resource",i={unversionedId:"client-guide/manage-bigquery-resource",id:"client-guide/manage-bigquery-resource",title:"Manage BigQuery Resource",description:"Below is the list of the resource types that Optimus supported:",source:"@site/docs/client-guide/manage-bigquery-resource.md",sourceDirName:"client-guide",slug:"/client-guide/manage-bigquery-resource",permalink:"/optimus/docs/client-guide/manage-bigquery-resource",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/client-guide/manage-bigquery-resource.md",tags:[],version:"current",lastUpdatedBy:"Arinda Arif",lastUpdatedAt:1721373701,formattedLastUpdatedAt:"Jul 19, 2024",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Installing Plugin in Client",permalink:"/optimus/docs/client-guide/installing-plugin"},next:{title:"Create Job Specifications",permalink:"/optimus/docs/client-guide/create-job-specifications"}},s={},c=[{value:"Dataset",id:"dataset",level:2},{value:"Table",id:"table",level:2},{value:"View",id:"view",level:2},{value:"External Table",id:"external-table",level:2},{value:"Upload Resource Specifications",id:"upload-resource-specifications",level:2}],p={toc:c},u="wrapper";function m(e){let{components:t,...n}=e;return(0,r.kt)(u,(0,a.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"manage-bigquery-resource"},"Manage BigQuery Resource"),(0,r.kt)("p",null,"Below is the list of the resource types that Optimus supported:"),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"Type"),(0,r.kt)("th",{parentName:"tr",align:null},"Description"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"dataset"),(0,r.kt)("td",{parentName:"tr",align:null},"Resource name format: ","[project]",".","[dataset]"," ",(0,r.kt)("br",null)," Spec can includes: table_expiration, description")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"table"),(0,r.kt)("td",{parentName:"tr",align:null},"Resource name format: ","[project]",".","[dataset]",".","[table]"," ",(0,r.kt)("br",null)," Spec can includes: schema, partition, cluster, description")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"view"),(0,r.kt)("td",{parentName:"tr",align:null},"Resource name format: ","[project]",".","[dataset]",".","[view]"," ",(0,r.kt)("br",null)," Spec can includes: view_query, description")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"external_table"),(0,r.kt)("td",{parentName:"tr",align:null},"Resource name format: ","[project]",".","[dataset]",".","[table]"," ",(0,r.kt)("br",null)," Spec can include: schema, source, description")))),(0,r.kt)("p",null,"You can create any of the above jobs using the same following format:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus resource create\n")),(0,r.kt)("p",null,"Make sure to put the correct resource type as you are intended. Once you fill in the command prompt questions, Optimus will create a file (resource.yaml) in the configured datastore directory. Below is an example of each of the type\u2019s resource specifications."),(0,r.kt)("h2",{id:"dataset"},"Dataset"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml"},'version: 1\nname: sample-project.playground\ntype: dataset\nlabels:\n  usage: documentation\n  owner: optimus\nspec:\n  description: "example description"\n  table_expiration: 24 # in hours\n')),(0,r.kt)("h2",{id:"table"},"Table"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml"},'version: 1\nname: sample-project.playground.sample_table\ntype: table\nlabels:\n  usage: documentation\n  owner: optimus\nspec:\n  description: "example description"\n  schema:\n  - name: column1\n    type: INTEGER\n  - name: column2\n    type: TIMESTAMP\n    description: "example field 2"\n    mode: required # (repeated/required/nullable), default: nullable\n  - name: column3\n    type: STRUCT\n    schema: # nested struct schema\n  - name: column_a_1\n    type: STRING\n  cluster:\n    using: [column1]\n  partition: # leave empty as {} to partition by ingestion time\n    field: column2 # column name\n    type: day # day/hour, default: day\n')),(0,r.kt)("h2",{id:"view"},"View"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml"},'version: 1\nname: sample-project.playground.sample_view\ntype: view\nlabels:\n  usage: documentation\n  owner: optimus\nspec:\n  description: "example description"\n  view_query: |\n    Select * from sample-project.playground.sample_table\n')),(0,r.kt)("h2",{id:"external-table"},"External Table"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml"},'version: 1\nname: sample-project.playground.sample_table\ntype: external_table\nlabels:\n  usage: documentation\n  owner: optimus\nspec:\n  description: "example description"\n  schema:\n  - name: column1\n    type: INTEGER\n  - name: column2\n    type: TIMESTAMP\n    description: "example field 2"\n  source:\n    type: google_sheets\n  uris:\n  - https://docs.google.com/spreadsheets/d/spreadsheet_id\n  config:\n    range: Sheet1!A1:B4 # Range of data to be ingested in the format of [Sheet Name]![Cell Range]\n    skip_leading_rows: 1 # Row of records to skip\n')),(0,r.kt)("h2",{id:"upload-resource-specifications"},"Upload Resource Specifications"),(0,r.kt)("p",null,"Once the resource specifications are ready, you can upload all resource specifications using the below command:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus resource upload-all --verbose\n")),(0,r.kt)("p",null,"The above command will try to compare the incoming resources to the existing resources in the server. It will create\na new resource if it does not exist yet, and modify it if exists, but will not delete any resources. Optimus does not\nsupport BigQuery resource deletion nor the resource record in the Optimus server itself yet."))}m.isMDXComponent=!0}}]);