"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[2106],{3905:(e,t,n)=>{n.d(t,{Zo:()=>d,kt:()=>m});var a=n(7294);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function r(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?r(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):r(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,o=function(e,t){if(null==e)return{};var n,a,o={},r=Object.keys(e);for(a=0;a<r.length;a++)n=r[a],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(a=0;a<r.length;a++)n=r[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var l=a.createContext({}),u=function(e){var t=a.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},d=function(e){var t=u(e.components);return a.createElement(l.Provider,{value:t},e.children)},c="mdxType",p={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},h=a.forwardRef((function(e,t){var n=e.components,o=e.mdxType,r=e.originalType,l=e.parentName,d=s(e,["components","mdxType","originalType","parentName"]),c=u(n),h=o,m=c["".concat(l,".").concat(h)]||c[h]||p[h]||r;return n?a.createElement(m,i(i({ref:t},d),{},{components:n})):a.createElement(m,i({ref:t},d))}));function m(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var r=n.length,i=new Array(r);i[0]=h;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s[c]="string"==typeof e?e:o,i[1]=s;for(var u=2;u<r;u++)i[u]=n[u];return a.createElement.apply(null,i)}return a.createElement.apply(null,n)}h.displayName="MDXCreateElement"},4365:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>i,default:()=>p,frontMatter:()=>r,metadata:()=>s,toc:()=>u});var a=n(7462),o=(n(7294),n(3905));const r={},i=void 0,s={unversionedId:"rfcs/revisit_automated_dependency_resolution",id:"rfcs/revisit_automated_dependency_resolution",title:"revisit_automated_dependency_resolution",description:"- Feature Name: Revisit Automated Dependency Resolution Logic",source:"@site/docs/rfcs/20220124_revisit_automated_dependency_resolution.md",sourceDirName:"rfcs",slug:"/rfcs/revisit_automated_dependency_resolution",permalink:"/optimus/docs/rfcs/revisit_automated_dependency_resolution",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/rfcs/20220124_revisit_automated_dependency_resolution.md",tags:[],version:"current",lastUpdatedBy:"Dery Rahman Ahaddienata",lastUpdatedAt:1698812879,formattedLastUpdatedAt:"Nov 1, 2023",sidebarPosition:20220124,frontMatter:{}},l={},u=[{value:"Background :",id:"background-",level:3},{value:"Approach :",id:"approach-",level:3},{value:"Other Thoughts:",id:"other-thoughts",level:3},{value:"How do we transition to this new approach?",id:"how-do-we-transition-to-this-new-approach",level:3}],d={toc:u},c="wrapper";function p(e){let{components:t,...n}=e;return(0,o.kt)(c,(0,a.Z)({},d,n,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"Feature Name: Revisit Automated Dependency Resolution Logic"),(0,o.kt)("li",{parentName:"ul"},"Status: Draft"),(0,o.kt)("li",{parentName:"ul"},"Start Date: 2022-01-24"),(0,o.kt)("li",{parentName:"ul"},"Authors: ")),(0,o.kt)("h1",{id:"summary"},"Summary"),(0,o.kt)("p",null,"Optimus is a data warehouse management system, data is at the core of Optimus. Automated dependency resolution is the core problem which Optimus wants to address. The current windowing is confusing, so lets take a revisit to understand how we can solve this problem if we are tackling it fresh."),(0,o.kt)("h1",{id:"technical-design"},"Technical Design"),(0,o.kt)("h3",{id:"background-"},"Background :"),(0,o.kt)("p",null,"Input Data Flows through the system & it is expected to arrive at some delay or user gives enough buffer for all late data to arrive. Post that, the user expects to schedule the job to process the data after the max delay."),(0,o.kt)("p",null,"Keeping this basic idea in mind, what logic can be used to enable automated dependency resolution is the key question for us? And what all questions need to be answered for the same?"),(0,o.kt)("p",null,"Question 1 : What is the time range of data a job consumes from the primary sources?"),(0,o.kt)("p",null,"Question 2 : What is the time range of data a job writes?"),(0,o.kt)("p",null,"If these two questions be answered for every scheduled job then dependent jobs be computed accordingly."),(0,o.kt)("h3",{id:"approach-"},"Approach :"),(0,o.kt)("p",null,"Let's answer the ",(0,o.kt)("strong",{parentName:"p"},"Question 1"),", this is clearly a user input, there is no computation here. How intuitively a user input can be taken is the key here."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-yaml"},"data_window : \n max_delay : 1d/2h/1d2h\n amount : 1d/1w/2d/1m \n")),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"max_delay")," is used to identify the end time of the window."),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"amount")," is the amount of data the user is consuming from the primary sources."),(0,o.kt)("p",null,"Below is the current windowing configuration."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-yaml"},"  window:\n    size: 24h\n    offset: 24h\n    truncate_to: d\n")),(0,o.kt)("p",null,"Let's answer ",(0,o.kt)("strong",{parentName:"p"},"Question 2"),", I believe this is mainly linked with the schedule of the job, if the job is scheduled daily then the expectation is the job makes data available for a whole day, if hourly then for a whole hour, irrespective of the input_window. What exactly is the time range can be computed by ",(0,o.kt)("inlineCode",{parentName:"p"},"max_delay")," and ",(0,o.kt)("inlineCode",{parentName:"p"},"schedule_frequency")," . "),(0,o.kt)("p",null,"If the job has a max_delay of 2h & the job is scheduled at 2 AM UTC then the job is making the data available for the entire previous day, irrespective of the window(1d,1m,1w)."),(0,o.kt)("p",null,"WIth this core idea there are few scenarios which should not be allowed or which cannot be used for automated depedency resolution to work in those cases the jobs just depend on the previous jobs for eg., dynamic schedules. If a job is scheduled only 1st,2nd & 3rd hour of the day."),(0,o.kt)("p",null,"The next part of the solution is how to do the actual dependency resolution."),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},"Compute the data_window based on user input & the schedule time."),(0,o.kt)("li",{parentName:"ol"},"Identify all the upstreams."),(0,o.kt)("li",{parentName:"ol"},"Starting from the current schedule_time get each upstream's schedule in the past & future and compute the output data range till it finds runs which falls outside the window in each direction.")),(0,o.kt)("h3",{id:"other-thoughts"},"Other Thoughts:"),(0,o.kt)("p",null,"Inorder to keep things simple for most of the users, if a user doesn't define any window then the jobs depend on the immediate upstream by schedule_time for that job & as well as the jobs that are depending the above job."),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"dstart")," & ",(0,o.kt)("inlineCode",{parentName:"p"},"dend")," macros will still be offered to users which they can use for data filtering in their queries."),(0,o.kt)("h3",{id:"how-do-we-transition-to-this-new-approach"},"How do we transition to this new approach?"),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},"Support Old & New approaches both, migrate all jobs to the new strategy & later cleanup the code.")))}p.isMDXComponent=!0}}]);