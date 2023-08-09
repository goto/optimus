"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[528],{3905:(e,t,a)=>{a.d(t,{Zo:()=>u,kt:()=>y});var r=a(7294);function n(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function l(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,r)}return a}function o(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?l(Object(a),!0).forEach((function(t){n(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):l(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function s(e,t){if(null==e)return{};var a,r,n=function(e,t){if(null==e)return{};var a,r,n={},l=Object.keys(e);for(r=0;r<l.length;r++)a=l[r],t.indexOf(a)>=0||(n[a]=e[a]);return n}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(r=0;r<l.length;r++)a=l[r],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(n[a]=e[a])}return n}var i=r.createContext({}),p=function(e){var t=r.useContext(i),a=t;return e&&(a="function"==typeof e?e(t):o(o({},t),e)),a},u=function(e){var t=p(e.components);return r.createElement(i.Provider,{value:t},e.children)},c="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var a=e.components,n=e.mdxType,l=e.originalType,i=e.parentName,u=s(e,["components","mdxType","originalType","parentName"]),c=p(a),m=n,y=c["".concat(i,".").concat(m)]||c[m]||d[m]||l;return a?r.createElement(y,o(o({ref:t},u),{},{components:a})):r.createElement(y,o({ref:t},u))}));function y(e,t){var a=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var l=a.length,o=new Array(l);o[0]=m;var s={};for(var i in t)hasOwnProperty.call(t,i)&&(s[i]=t[i]);s.originalType=e,s[c]="string"==typeof e?e:n,o[1]=s;for(var p=2;p<l;p++)o[p]=a[p];return r.createElement.apply(null,o)}return r.createElement.apply(null,a)}m.displayName="MDXCreateElement"},2844:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>i,contentTitle:()=>o,default:()=>d,frontMatter:()=>l,metadata:()=>s,toc:()=>p});var r=a(7462),n=(a(7294),a(3905));const l={},o="Replay a Job (Backfill)",s={unversionedId:"client-guide/replay-a-job",id:"client-guide/replay-a-job",title:"Replay a Job (Backfill)",description:"Some old dates of a job might need to be re-run (backfill) due to business requirement changes, corrupt data, or other",source:"@site/docs/client-guide/replay-a-job.md",sourceDirName:"client-guide",slug:"/client-guide/replay-a-job",permalink:"/optimus/docs/client-guide/replay-a-job",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/client-guide/replay-a-job.md",tags:[],version:"current",lastUpdatedBy:"Sandeep Bhardwaj",lastUpdatedAt:1691570012,formattedLastUpdatedAt:"Aug 9, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Backup BigQuery Resource",permalink:"/optimus/docs/client-guide/backup-bigquery-resource"},next:{title:"Work with Extension",permalink:"/optimus/docs/client-guide/work-with-extension"}},i={},p=[{value:"Run a replay",id:"run-a-replay",level:2},{value:"Get a replay status",id:"get-a-replay-status",level:2},{value:"Get list of replays",id:"get-list-of-replays",level:2}],u={toc:p},c="wrapper";function d(e){let{components:t,...a}=e;return(0,n.kt)(c,(0,r.Z)({},u,a,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("h1",{id:"replay-a-job-backfill"},"Replay a Job (Backfill)"),(0,n.kt)("p",null,"Some old dates of a job might need to be re-run (backfill) due to business requirement changes, corrupt data, or other\nvarious reasons. Optimus provides a way to do this using Replay. Please go through ",(0,n.kt)("a",{parentName:"p",href:"/optimus/docs/concepts/replay-and-backup"},"concepts")," to know more about it."),(0,n.kt)("h2",{id:"run-a-replay"},"Run a replay"),(0,n.kt)("p",null,"To run a replay, run the following command:"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus replay create {job_name} {start_time} {end_time} [flags]\n")),(0,n.kt)("p",null,"Example:"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus replay create sample-job 2023-03-01T00:00:00Z 2023-03-02T15:00:00Z --parallel --project sample-project --namespace-name sample-namespace\n")),(0,n.kt)("p",null,"Replay accepts three arguments, first is the DAG name that is used in Optimus specification, second is the scheduled\nstart time of replay, and third is the scheduled end time (optional) of replay."),(0,n.kt)("p",null,"Once your request has been successfully replayed, this means that Replay has cleared the requested runs in the scheduler.\nPlease wait until the scheduler finishes scheduling and running those tasks."),(0,n.kt)("h2",{id:"get-a-replay-status"},"Get a replay status"),(0,n.kt)("p",null,"You can check the replay status using the replay ID given previously and use in this command:"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus replay status {replay_id} [flag]\n")),(0,n.kt)("p",null,"You will see the latest replay status including the status of each run of your replay."),(0,n.kt)("h2",{id:"get-list-of-replays"},"Get list of replays"),(0,n.kt)("p",null,"List of recent replay of a project can be checked using this sub command:"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus replay list [flag]\n")),(0,n.kt)("p",null,"Recent replay ID including the job, time window, replay time, and status will be shown. To check the detailed status\nof a replay, please use the status sub command."))}d.isMDXComponent=!0}}]);